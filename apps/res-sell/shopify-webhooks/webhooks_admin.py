import os, sys
import time
import json
import requests

# Check if API Key is set, if not import secrets_client and load (for local usage)
graph_api_key = os.getenv("GRAPH_API_KEY", None)
if not graph_api_key:
    from res.utils import secrets_client

    graph_api_key = secrets_client.get_secret("GRAPH_API_KEY")

HEADERS = {
    "x-api-key": graph_api_key,
    "apollographql-client-name": "lambda",
    "apollographql-client-version": os.getenv("AWS_LAMBDA_FUNCTION_NAME", "test"),
}


def graph_request(query, variables):
    response = requests.post(
        os.getenv("GRAPH_API", "https://api.resmagic.io/graphql"),
        json={"query": query, "variables": variables},
        headers=HEADERS,
    )
    if response.status_code == 200:
        data = response.json()
        return data.get("data")
    else:
        print("NOT FOUND IN GRAPH!")
        raise ValueError("ERROR IN GRAPH REQUEST")


RESONANCE_WEBHOOK_URL = "https://data.resmagic.io/shopify-webhooks"
RESONANCE_DEV_WEBHOOK_URL = "https://datadev.resmagic.io/shopify-webhooks"

TOPICS = [
    {"topic": "orders/create", "route": "create-order"},
    {"topic": "orders/updated", "route": "update-order"},
    {"topic": "orders/cancelled", "route": "cancel-order"},
    {"topic": "products/create", "route": "create-product"},
    {"topic": "products/update", "route": "update-product"},
    {"topic": "products/delete", "route": "delete-product"},
    {"topic": "bulk_operations/finish", "route": "bulk-operations-finish"},
]

GET_BRAND = """
    query getBrandStoreCredentials($where: BrandsWhere) {
        brands(first: 1, where: $where) {
            brands {
                id
                code
                shopifyApiKey
                shopifyApiPassword
                shopifyStoreName
            }
        }
    }
"""

GET_DATA_SOURCE_INSTANCE = """
    query getInstance($where: DataSourceInstancesWhere){
        dataSourceInstances(first: 1, where: $where) {
            dataSourceInstances {
                id
                dataSource {
                    id
                    name
                }
                    isEnabled
            }
        }
    }
      
"""

UPDATE_DATA_SOURCE_INSTANCE_MUTATION = """
    mutation updateDataSourceInstance($id: ID!, $input: UpdateDataSourceInstanceInput!) {
        updateDataSourceInstance(id: $id, input: $input) {
            dataSourceInstance {
                id
            }
        }
    }
"""

CREATE_WEBHOOK_MUTATION = """
mutation webhookSubscriptionCreate($topic: WebhookSubscriptionTopic!, $webhookSubscription: WebhookSubscriptionInput!) {
  webhookSubscriptionCreate(
    topic: $topic
    webhookSubscription: $webhookSubscription
  ) {
    userErrors {
      field
      message
    }
    webhookSubscription {
      id
    }
  }
}
"""


def get_store_credentials(brand_id):
    response = graph_request(GET_BRAND, {"where": {"code": {"is": brand_id}}})
    return response["brands"]["brands"][0]


def delete_webhooks(credentials):
    base_url = f"https://{credentials.get('shopifyApiKey')}:{credentials.get('shopifyApiPassword')}@{credentials.get('shopifyStoreName')}.myshopify.com/admin/api/2022-04"
    get_webhooks_url = base_url + "/webhooks.json"

    response = requests.get(get_webhooks_url)
    webhooks = json.loads(response.content)["webhooks"]
    for webhook in webhooks:
        delete_webhooks_url = base_url + f"/webhooks/{webhook['id']}.json"
        response = requests.delete(delete_webhooks_url)
        print(response)


def view_webhooks(credentials):
    base_url = f"https://{credentials.get('shopifyApiKey')}:{credentials.get('shopifyApiPassword')}@{credentials.get('shopifyStoreName')}.myshopify.com/admin/api/2022-04"
    get_webhooks_url = base_url + "/webhooks.json"

    response = requests.get(get_webhooks_url)
    webhooks = json.loads(response.content)["webhooks"]
    print("### Shopify Store Name:" + credentials.get("shopifyStoreName"))
    for webhook in webhooks:
        print(json.dumps(webhook, indent=2))


def create_webhooks(credentials, brand_code, env="production"):
    url = f"https://{credentials.get('shopifyApiKey')}:{credentials.get('shopifyApiPassword')}@{credentials.get('shopifyStoreName')}.myshopify.com/admin/api/2022-04/webhooks.json"
    if env == "development":
        webhook_url = RESONANCE_DEV_WEBHOOK_URL
    else:
        webhook_url = RESONANCE_WEBHOOK_URL

    for topic in TOPICS:
        payload = {
            "webhook": {
                "topic": topic.get("topic"),
                "address": f"{webhook_url}/{topic.get('route')}?brand_code={credentials.get('code')}",
                "format": "json",
            }
        }
        response = requests.post(
            url,
            json=payload,
            auth=(
                credentials.get("shopifyApiKey"),
                credentials.get("shopifyApiPassword"),
            ),
        )
        print(topic.get("topic", "") + ": " + response.text)
        time.sleep(0.3)
    response = graph_request(
        GET_DATA_SOURCE_INSTANCE,
        {
            "where": {
                "instanceCode": {"is": brand_code},
                "isEnabled": {"is": True},
                "dataSourceCode": {"is": "Shopify"},
            }
        },
    )
    instance_id = response["dataSourceInstances"]["dataSourceInstances"][0]["id"]

    response = graph_request(
        UPDATE_DATA_SOURCE_INSTANCE_MUTATION,
        {"id": instance_id, "input": {"isPullingDataFromShopify": True}},
    )


def handler(event, context):
    brand_code = event.get("brand_code")
    # Default to production/create, used in Lambda triggered by Airtable
    env = event.get("env", "production")
    action = event.get("action", "create")
    credentials = get_store_credentials(brand_code)
    if action == "delete":
        delete_webhooks(credentials)
    elif action == "create":
        create_webhooks(credentials, brand_code, env)
    elif action == "view":
        view_webhooks(credentials)


if __name__ == "__main__":
    # First argument is brand code, second is action
    if len(sys.argv) < 3:
        print(
            "Run this script with 2 arguments: 'brand_code' 'action' and an optional 3rd Argument: 'env' which can be development or production. Action can be view, create, delete"
        )
        sys.exit()
    event = {"brand_code": sys.argv[1], "action": sys.argv[2]}

    if len(sys.argv) > 3:
        event["env"] = sys.argv[3]
    handler(event, {})
