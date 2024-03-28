import res
from collections.abc import MutableMapping
from elasticsearch import Elasticsearch
import pandas as pd


def flatten(dictionary, parent_key="", separator="_"):
    items = []
    for key, value in dictionary.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten(value, new_key, separator=separator).items())
        else:
            items.append((new_key, value))
    return dict(items)


class Elastic:
    def __init__(self):
        # temp - assume port open for test cluster
        self.es = Elasticsearch(
            hosts=["https://localhost:9200"],
            basic_auth=("elastic", "4I9IFdj8F0R9PXW46Kc925OF"),
            verify_certs=False,
        )

        # idx = 'logstashadmission-2023.05.23'
        # es.indices.exists(index=idx)
        # r = es.indices.get_alias(index='*')

    def _test_search_logs_index(self, index, query_body):
        """
        illustrates search the logs of an app: we should specify dates etc to make this useful
        """
        idx = index or "logstashadmission-2023.05.23"
        query_body = query_body or {
            "match": {"kubernetes.labels.app": "ask-one-bot"},
        }

        res = self.s.search(
            index=idx,
            query=query_body,
            source_includes=["kubernetes.pod.name", "kubernetes.labels", "message"],
        )

        return pd.DataFrame([flatten(item) for item in res.body["hits"]["hits"]])
