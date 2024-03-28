import res


def get_brands():
    graph = res.connectors.load("graphql")

    BRAND_CREDENTIALS_QUERY = """
    query brandShopifyCredentials($after:String) {
        brands(first:100, after:$after) {
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
    # Ideally the following filtering should occur entirely within the WHEREVALUE of the graph querry
    result = graph.query(BRAND_CREDENTIALS_QUERY, {}, paginate=True)
    for each in result["data"]["brands"]["brands"]:
        if each["shopifyApiKey"] == "" or each["shopifyApiKey"] == None:
            continue
        else:
            creds_array.append(each)
    return creds_array


def get_brand(code):
    graph = res.connectors.load("graphql")

    BRAND_CREDENTIALS_QUERY = f"""
    query brandShopifyCredentials($after:String) {{
        brands(first:100, after:$after, where:{{code: {{is: "{code}"}}}}) {{
            brands{{
            name
            code
            shopifyStoreName
                shopifyApiKey
                shopifyApiPassword
            }}
        count
        cursor
        }}
    }}
    """

    creds_array = []
    # Ideally the following filtering should occur entirely within the WHEREVALUE of the graph querry
    result = graph.query(BRAND_CREDENTIALS_QUERY, {}, paginate=True)
    for each in result["data"]["brands"]["brands"]:
        if each["shopifyApiKey"] == "" or each["shopifyApiKey"] == None:
            continue
        else:
            creds_array.append(each)
    return creds_array
