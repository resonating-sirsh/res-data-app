class FakeGraphQl:
    def __init__(self, response):
        self.response = response
        self.query_string = None
        self.variables = None

    def query(self, query, variables):
        self.query_string = query
        self.variables = variables
        return self.response(query, variables)
