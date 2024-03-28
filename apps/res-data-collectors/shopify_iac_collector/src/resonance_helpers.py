from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import logger


def is_resonance_item(sku):
    """Determines if item is a resonance item based on sku. Resonance skus need
    to have 4 parts, the first part being 6 characters, e.g. TK6074 SLCTN FLOWJY 1ZZXS
    """
    if sku is None:
        return False
    parts = sku.split(" ")
    if len(parts) != 4:
        return False
    if len(parts[0]) != 6:
        return False
    return True


def get_brands(code=None):
    """Pulls down brand information from Airtable via the Graph API.
    If code is supplied, only return that brand 's credentials"""

    graph = ResGraphQLClient()

    brand_credentials_query = """
    query brandShopifyCredentials($after:String) {
        brands(first:100, after:$after, where: {isActive: {is: true}}) {
            brands{
                name
                code
                shopifyStoreName
                shopifyApiKey
                shopifyApiPassword
            }
        count
        cursor
        }
    }
    """

    creds_array = []
    result = graph.query(brand_credentials_query, {}, paginate=True)
    for each in result["data"]["brands"]["brands"]:
        if each["shopifyApiKey"] == "" or each["shopifyApiKey"] is None:
            logger.warning("Brand does not have a shopify Api Key: " + str(each))
            continue
        if code is None or each["code"] == code:
            creds_array.append(each)
    return creds_array
