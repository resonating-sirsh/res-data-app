# This is a Resonance GraphQL Client, which runs queries against
# the Resonance GraphQL APIs

import os

import requests

from res.utils import logger, secrets_client

MAX_RETRIES = 3

CREATE_ISSUE_MUTATION = """
    mutation createIssues($input: CreateIssueInput!) {
    createIssues(input: $input) {
      issues{
            id
      }        
    }
}
"""


class GraphQLException(Exception):
    pass


class ResGraphQLClient:
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(ResGraphQLClient, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

    def __init__(
        self,
        process_name=os.getenv("RES_APP_NAME", "unlabeled"),
        api_key=None,
        api_url=None,
        environment=None,
    ):
        # Check for env overrides, use development if none found
        if environment:
            self.environment = environment
        else:
            self.environment = os.getenv("RES_ENV", "development")

        # Initialize GraphQL
        self.process_name = process_name
        if api_key:
            self.api_key = api_key
        else:
            self.api_key = secrets_client.get_secret("GRAPH_API_KEY")

        if api_url:
            self.graph_api_url = api_url
        else:
            self.graph_api_url = os.getenv(
                "GRAPHQL_API_URL", "https://api.resmagic.io/graphql"
            )

    def query_with_kwargs(self, query, paginate=False, path=None, **kwargs):
        """
        provide a wrapper around query to pass keyword args

        query: the query
        paginate: paginate or not
        path: a way to pop the request from the nested json (see example)
        **kwargs: keyword args/variables passed to the query

        Example:
            from res.connectors.graphql import ResGraphQL, Queries
            ResGraphQL().query_with_kwargs(Queries.GET_ASSET_REQUEST, path='data.assetRequest', id='recHw1LRGkhHkU4df')

        """
        result = self.query(query, paginate=paginate, variables=kwargs)

        if path:
            for p in path.split("."):
                result = result[p]

        return result

    def query(self, query, variables, paginate=False):
        json_data = {}
        if paginate:
            entries = []
            variables["after"] = "0"
            cursor = 0
            count = 1
            endpoint = ""
            while cursor < count:
                attempt = 1
                while attempt <= MAX_RETRIES:
                    response = requests.post(
                        self.graph_api_url,
                        json={"query": query, "variables": variables},
                        headers={
                            "x-api-key": self.api_key,
                            "apollographql-client-name": "test",
                            # 'res-data-platform',
                            "apollographql-client-version": "test"
                            # self.process_name
                        },
                    )
                    json_data = response.json()
                    if (
                        "message" in json_data
                        and json_data["message"] == "Endpoint request timed out"
                    ):
                        if attempt == MAX_RETRIES:
                            logger.error("ENDPOINT REQUEST TIMED OUT")
                            raise GraphQLException("ENDPOINT REQUEST TIMED OUT")
                        attempt += 1
                        logger.warn(f"Requested Timed Out Retrying for {attempt}th ")
                    else:
                        break
                print(json_data)
                endpoint = list(json_data["data"].keys())[0]
                count = 0
                cursor = 0
                if json_data["data"][endpoint].get("count"):
                    count = int(json_data["data"][endpoint]["count"])

                if json_data["data"][endpoint].get("cursor"):
                    cursor = (
                        int(json_data["data"][endpoint]["cursor"])
                        if json_data["data"][endpoint]["cursor"]
                        else 1
                    )

                logger.info(f"Cursor:{cursor} of Count:{count}")

                variables["after"] = str(cursor)
                if json_data["data"][endpoint].get(endpoint):
                    entries = entries + (json_data["data"][endpoint][endpoint])
                else:
                    entries.append(json_data["data"][endpoint])

            json_data["data"][endpoint][endpoint] = entries

        else:
            response = requests.post(
                self.graph_api_url,
                json={"query": query, "variables": variables},
                headers={
                    "x-api-key": self.api_key,
                    "apollographql-client-name": "test",
                    # 'res-data-platform',
                    "apollographql-client-version": "test"
                    # self.process_name
                },
            )

            json_data = response.json()

        if "errors" in json_data:
            raise GraphQLException(json_data["errors"])

        return json_data

    def submit_issue(
        self,
        context,
        subject,
        dxa_node_id,
        issue_type_ids,
        source_record_id,
        issue_type="Flag for Review",
        owner_email="techpirates@resonance.nyc",
    ):
        self.query(
            CREATE_ISSUE_MUTATION,
            {
                "input": {
                    "context": context,
                    "subject": subject,
                    "dxaNodeId": dxa_node_id,
                    "issueTypesIds": issue_type_ids,
                    "type": issue_type,
                    "ownerEmail": owner_email,
                    "sourceRecordId": source_record_id,
                }
            },
        )
