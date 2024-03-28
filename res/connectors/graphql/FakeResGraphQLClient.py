import os, json, requests, logging

# This is a FAKE Resonance GraphQL Client, which __emulates__ running queries against
# the Resonance GraphQL APIs


class FakeResGraphQLClient:
    def __init__(self, process_name, api_key, api_url, environment):
        self.environment = environment
        self.logger = logging.getLogger(__name__)
        if self.environment == "production":
            self.logger.setLevel(logging.INFO)
        else:
            self.logger.setLevel(logging.DEBUG)
        self.logger.debug(
            "Initialized client, environment: {}".format(self.environment)
        )
        self.process_name = process_name
        self.api_key = api_key
        self.graph_api_url = api_url

    def query(self, query, variables):
        if "query Body(" in query:
            # Find body query
            if variables["number"] == "CC-2028":
                return {"data": {"body": True}}
            else:
                return {}
        return {}


class MockGraphQl:
    def __init__(self, response):
        self.response = response
        self.query_string = None
        self.variables = None

    def query(self, query, variables):
        self.query_string = query
        self.variables = variables
        return self.response(query, variables)
