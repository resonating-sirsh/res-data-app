from res.utils import logger
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient

GET_LINE_ITEMS_QUERY = """
query GetOrderItems($brand:WhereValue,$startDate:WhereValue,$endDate:WhereValue,$after:String){
		orderLineItems(first:50 after:$after where: {brandCode: {is: $brand} createdAt:{isGreaterThanOrEqualTo: $startDate, isLessThan: $endDate}}){
    orderLineItems{
      brand{code}
      order{
        salesChannel
        number
        channelOrderId
      }
      createdAt
      timestampFulfillment
      name
      activeOneNumber
    }
    count
    cursor
    }
}"""

# Basic query functionality
if __name__ == "__main__":
    client = ResGraphQLClient()
    logger.debug("Executing query...")
    brand_code = "OO"
    start_date = "2021-01-01"
    end_date = "2021-02-01"
    # Running the query. This inserts the variables into the query, executes, and uses
    # _optional_ pagination to get all results
    results = client.query(
        GET_LINE_ITEMS_QUERY,
        {"brand": brand_code, "startDate": str(start_date), "endDate": str(end_date)},
        paginate=True,
    )
    for row in results:
        logger.debug(row)
    logger.debug("Done!")
