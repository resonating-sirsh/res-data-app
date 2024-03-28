from multiprocessing import get_context
import os, json
from res.utils import logger
from res.utils.flask.ResFlask import ResFlask
from flask_graphql import GraphQLView
from schema import schema

from res.connectors.s3 import S3Connector

s3 = S3Connector()

__version__ = 0.1

logger.info("Setting up flask app...")
# ResFlask is a wrapper around Flask. It sets up:
#   - metrics exporting, so we can view data in Grafana
#   - trace logging, so we can trace calls thru the stack
#   - CORS so we can call the app from other websites
#         > specify each domain you want to allow requests from
#   - Authentication via Cognito User Pools
app = ResFlask(
    __name__,
    enable_trace_logging=True,
    allowed_cors_domains=[
        "https://create.one",
    ],
)

app.add_url_rule(
    "/graphql",
    view_func=GraphQLView.as_view(
        "graphql", schema=schema, graphiql=True, get_context=lambda: {"s3": s3}
    ),
)

if __name__ == "__main__":
    app.run(host="0.0.0.0")
