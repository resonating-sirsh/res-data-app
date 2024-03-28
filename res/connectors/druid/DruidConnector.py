# https://pythonhosted.org/pydruid/

from . import DatabaseConnector, DatabaseConnectorSchema, DatabaseConnectorTable
from pydruid.db import connect
import pandas as pd
import pandas as pd
import os

# TODO
DRUID_PORT = os.environ.get("DRUID_PORT", 8005)
DRUID_HOST = os.environ.get("DRUID_HOST", "localhost")
# turn this into something create ingestion specs from res meta schema
# in the case of time series you want to just count and the rest as dims
# we could auto add floats and ints with some blacklist
#
sample_kafka_time_Series = {
    "type": "kafka",
    "spec": {
        "ioConfig": {
            "type": "kafka",
            "consumerProperties": {
                "bootstrap.servers": "b-2.res-primary-dev.0hn5z0.c13.kafka.us-east-1.amazonaws.com:9094,b-3.res-primary-dev.0hn5z0.c13.kafka.us-east-1.amazonaws.com:9094,b-1.res-primary-dev.0hn5z0.c13.kafka.us-east-1.amazonaws.com:9094",
                "security.protocol": "SSL",
                "ssl.truststore.location": "/usr/lib/jvm/java-1.8.0-openjdk-amd64/jre/lib/security/cacerts",
            },
            "topic": "makeNodeResourceEvents",
            "useEarliestOffset": True,
        },
        "tuningConfig": {"type": "kafka", "reportParseExceptions": True},
        "dataSchema": {
            "dataSource": "makeNodeResourceEvents",
            "parser": {
                "type": "avro_stream",
                "avroBytesDecoder": {
                    "type": "schema_registry",
                    "url": "http://kafka-schema-registry-cp-schema-registry.kafka-schema-registry.svc.cluster.local:8081",
                    "capacity": 100,
                },
                "parseSpec": {
                    "format": "avro",
                    "timestampSpec": {"column": "timestamp", "format": "auto"},
                    "dimensionsSpec": {
                        "dimensions": ["name", "resource_status", "resource_type"],
                        "dimensionExclusions": [],
                    },
                },
            },
            "granularitySpec": {
                "type": "uniform",
                "queryGranularity": "MINUTE",
                "segmentGranularity": "DAY",
                "rollup": False,
            },
            "metricsSpec": [{"name": "count", "type": "count"}],
        },
    },
}


class DruidConnector(DatabaseConnector):
    """
    The druid connector will be refactored as the druid usage evolves
    """

    def __init__(self):
        pass

    # temp should think about how to structure the connector but druid pattern not yes established

    def _get_client(self):
        conn = connect(
            host=DRUID_HOST, port=DRUID_PORT, path="/druid/v2/sql/", scheme="http"
        )
        return conn

    def execute(self, query):
        curs = self._get_client().cursor()
        result = curs.execute(query)
        return pd.DataFrame(result)
