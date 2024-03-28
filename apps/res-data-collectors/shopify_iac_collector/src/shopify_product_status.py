import requests, json, time

from res.utils import logger


def get_online_store_status(shopify_api_key, store_name):
    headers = {
        "X-Shopify-Access-Token": shopify_api_key,
    }
    url = f"https://{store_name}.myshopify.com/admin/api/2022-04/graphql.json"

    # Get publications
    pub_query = """query {
    publications (first: 20) {
        edges {
            node {
                id
                name
            }
        }
    }
    }"""
    publications = requests.post(url, headers=headers, json={"query": pub_query}).json()

    # Get publication details for products
    product_query = """
    query ($publication_id: ID!, $after: String) {
        products(first: 250, after: $after) {
            edges {
                cursor
                node {
                    id
                    publishedOnPublication (publicationId: $publication_id)
                }
            }
            pageInfo {
                hasNextPage
            }
        }
    } """

    # Check if this store has access to publications
    if "errors" in publications and len(publications["errors"]) > 0:
        if "Access denied" in publications["errors"][0]["message"]:
            logger.warn(json.dumps(publications))
            logger.warn(
                "Resonance does not have access to brand's publications! Need to request from Shopify support"
            )
            return {}
        else:
            logger.error("Unknown error from shopify: ")
            logger.info(json.dumps(publications))
            return {}
    # Loop thru publications, if we find Online store check to see status
    for publication in publications["data"]["publications"]["edges"]:
        if publication["node"]["name"] == "Online Store":
            variables = {"publication_id": publication["node"]["id"]}
            # Paginate results
            page = 0
            has_more = True
            cursor = None
            results = {}
            while has_more:
                if not cursor:
                    variables["after"] = None
                else:
                    variables["after"] = cursor
                response = requests.post(
                    url,
                    headers=headers,
                    json={"query": product_query, "variables": variables},
                ).json()
                if "data" not in response:
                    # Check for throttling
                    if (
                        "errors" in response
                        and len(response["errors"]) > 0
                        and "message" in response["errors"][0]
                        and response["errors"][0]["message"] == "Throttled"
                    ):
                        logger.warn("Throttled by Shopify, waiting 10 seconds...")
                        time.sleep(10)
                        continue
                    else:
                        logger.error(
                            f"Bad response from Shopify! {json.dumps(response, indent=2)}"
                        )
                        return {}
                for item in response["data"]["products"]["edges"]:
                    cursor = item["cursor"]
                    product_id = item["node"]["id"].split("/")[-1]
                    results[product_id] = item["node"]["publishedOnPublication"]
                page += 1
                logger.info(f"Page {page}...")
                if not response["data"]["products"]["pageInfo"]["hasNextPage"]:
                    has_more = False
            return results
    return {}
