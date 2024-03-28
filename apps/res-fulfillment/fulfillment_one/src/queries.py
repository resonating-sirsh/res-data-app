# can you check everything in this query is needed. eg i looked at standardPrice and its not being used. THese createOne queries are slow AF so stripping them to the bare essentails is a good idea
# separately it looks like most of the orders stuff in fulfillment_one_utils can now go away. will you make sure that is deleted when you merge this - we dont want to have to think we have to maintan all that code too - i.e keep the technical debt low where possible
GET_STYLE = """
query getStyleNpMapped($after: String, $where: StylesWhere!) {
  styles(first: 100, after: $after, where: $where) {
    styles {
      id      
      name
      code      
      isOneReady
      isStyle3dOnboarded
      is3dColorPlacementEnabled
      createdAt
      version
      material {
        id
        code
      }
      body {
        id
        code
        patternVersionNumber
      }
      color {
        id
        code
      }
    }
    hasMore
    cursor
    count
  }
}
"""

GET_PRODUCT_PRICE = """
query getProductPrice($where: ProductsWhere) {
  products(first: 100, where: $where) {
    products {
      id
      price
      wholesalePrice
    }
  }
}
"""

GET_STYLE_PRICE = """
query style($id: ID! $sizeCodes: [String]) {
  style(id: $id) {
    id
    code
    name
    onePrices(sizeCodes: $sizeCodes) {
      cost      
      priceBreakdown {
        item
        category
        rate
        quantity
        unit
        cost
      }
    }
  }
}
"""

QUERY_VALIDATE_STYLE_3D = """
query validateIfStyleIs3D($id: ID!){
  style(id: $id){
    id
    isStyle3dOnboarded
    is3dColorPlacementEnabled
    createdAt
  }
}
"""

QUERY_GET_ORDER_LINE_ITEM = """
query getOrderLineItem($id: ID!){
    orderLineItem(id:$id) {
        id
        sku
        flagForReviewTags
        styleCode
        salesChannel
        friendlyName
        requestType
        strikeOffCount
        hasGroupFulfillment
        fulfillmentStatus
        inventoryCheckinType
        channelOrderLineItemId
        friendlyName
        isAnExtraItem
        fulfillment{
          id
        }
        style {
          id
          name
          code
          oneReadyStatus
          isOneReady
          standardPrice
          version
          material {
            id
            code
          }
          body {
            id
            code
            patternVersionNumber
          }
          color {
            id
            code
          }
          styleBillOfMaterials {
            billOfMaterial {
              id
              name
            }
            trimItem {
              id
              longName
              name
            }
          }
        }
        brand {
          id
          activeSubscriptionId
          code
          name
        }
        order {
          id
          salesChannel
          requestType
          number
          channelOrderId
          requestName
          orderChannel
          friendlyName
          wasPaymentSuccessful
        }
        size {
          id
          name
          code
        }
        material {
          id
          code
          name
        }
        color {
          id
          code
        }
        artworkFiles {
          id
          name
        }
        createdAt
    }
}
"""


GET_BRAND_INFO = """
query getBrand($where: BrandsWhere!) {
  brands(first: 1, where: $where) {
    brands {
      id
      code
      name
      activeSubscriptionId
    }
  }
}
"""

GET_INVENTORY_UNIT = """
query findUnits($where: UnitsWhere!){
  units(first:1, where:$where){
    units {
      id
      binLocation
      warehouseBinLocations
      warehouseCheckinLocation
      resMagicOrderNumber
    }
    count
  }
}
"""

UPDATE_INVENTORY_UNIT = """
  mutation updateUnit($id: ID!, $input: UpdateUnitInput!){
    updateUnit(id:$id, input:$input) {
      unit {
        id
      }
    }
  }
"""

GET_ORDERS_BY_BRAND_AND_DATE = """
SELECT 
    so.id, 
    so.created_at, 
    so.updated_at, 
    so.customer_id, 
    so.ecommerce_source, 
    so.source_order_id, 
    so.ordered_at, 
    so.brand_code, 
    so.source_metadata, 
    so.sales_channel, 
    so.order_channel, 
    so.email, 
    so.tags, 
    so.item_quantity_hash, 
    so.name, 
    so.status, 
    so.contracts_failing, 
    so.deleted_at, 
    so.was_payment_successful, 
    so.was_balance_not_enough,
    soli.id AS line_item_id, 
    soli.created_at AS line_item_created_at, 
    soli.updated_at AS line_item_updated_at, 
    soli.order_id, 
    soli.source_order_line_item_id, 
    soli.sku, 
    soli.quantity, 
    soli.price, 
    soli.source_metadata AS line_item_source_metadata, 
    soli.status AS line_item_status, 
    soli.customization, 
    soli.revenue_share_code, 
    soli.metadata AS line_item_metadata, 
    soli.name AS line_item_name, 
    soli.product_id, 
    soli.fulfillable_quantity, 
    soli.fulfilled_quantity, 
    soli.refunded_quantity

FROM SELL.orders so
INNER JOIN SELL.order_line_items soli ON so.id = soli.order_id
WHERE so.brand_code = %s
AND so.created_at >= %s
AND so.created_at <= %s 
AND deleted_at IS NULL;

"""

"""
By joining a bridge table that mirrors the make one production base
we can get the status of each one linked to the order, including cancelled ones
We should do a checksum in business tier to ensure correct number of active ONES for each sKU 
"""
GET_ORDER_PRODUCTION_REQUESTS = """
 SELECT   
      mone.order_number,
      o.ordered_at,
      oi.name as product_name,
      mone.sku,
      o.sales_channel,
      o.order_channel,
      email,
      quantity,
      make_node,
      assembly_node,
      airtable_rid as mone_airtable_id,
      print_request_airtable_id
      dxa_assets_ready_at,
      exited_cut_at,
      exited_print_at,
      exited_sew_at,
      mone.created_at as production_request_created_at,
      cancelled_at as production_request_cancelled_at,
      mone.contracts_failing,
      pieces_currently_healing,
      --TODO: this should be provided by fulf team - could be added to the bridge table or read from the order item
      fulfilled_at
    --
    FROM sell.orders o 
      JOIN sell.order_line_items oi ON o.id = oi.order_id
      JOIN infraestructure.bridge_sku_one_counter mone ON mone.order_number = o.name and mone.sku = oi.sku
      --TODO: risk of swaps of sku names if we join on the sku
      --may want to consider another join criterion
    where o.name = %s
      --TODO: could filter on cancelled or leave for business tier - we can over count here if we dont
      --and mone.cancelled_at is NONE
  
  """
