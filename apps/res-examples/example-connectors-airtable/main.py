from res.utils import logger
from res.connectors.airtable.AirtableClient import ResAirtableClient

BRAND_BASE_ID = "appc7VJXjoGsSOjmw"
BRAND_TABLE_ID = "tblMnwRUuEvot969I"

if __name__ == "__main__":
    # Initialize the client
    client = ResAirtableClient()

    ## Example 1: Query a full table from Airtable
    logger.info("Executing full table query...")
    results = client.get_records(BRAND_BASE_ID, BRAND_TABLE_ID)
    for row in results:
        if row["fields"].get("Code", None) == "TT":
            # Print row if it matches brand Techpirates Testing
            logger.info(row)

    ## Example 2: Using a filter forumla
    logger.info("Executing filter query...")
    results = client.get_records(
        BRAND_BASE_ID, BRAND_TABLE_ID, filter_by_formula='Code="TT"'
    )
    logger.info(results)
    logger.info("Done!")
