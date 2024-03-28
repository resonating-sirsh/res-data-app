"""

https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/rayservice.html
https://raw.githubusercontent.com/ray-project/kuberay/v1.0.0/ray-operator/config/samples/ray_v1alpha1_rayservice.yaml
https://ray-project.github.io/q4-2021-docs-hackathon/0.2/ray-ml/ray-data/tutorials/

"""
import res
import ray
from ray import serve
from res.observability.io import VectorDataStore, ColumnarDataStore
from res.observability.entity import AbstractEntity, AbstractVectorStoreEntry

COL_STORES = [
    "queues.Body",
    "queues.Style",
    "queues.Order",
    "queues.ProductionRequest",
    "knowledge_base.glossary",
]
VEC_STORES = [
    "coda.make_svot",
    "coda.resonance-genome",
    "coda.resonance-contracts",
]


class Store:
    def __init__(self):
        self._db = {}

        for fullname in COL_STORES:
            namespace = fullname.split(".")[0]
            name = fullname.split(".")[1]

            self._db[fullname] = ColumnarDataStore(
                AbstractEntity.create_model(name, namespace)
            )

        for fullname in VEC_STORES:
            namespace = fullname.split(".")[0]
            name = fullname.split(".")[1]

            self._db[fullname] = VectorDataStore(
                AbstractEntity.create_model(name, namespace)
            )

    def __getitem__(self, key):
        return self._db[key]

    def search_many(self, question, stores, **kwargs):
        return [self[s].run_search(question, **kwargs) for s in stores]

    def __call__(self, question, stores, **kwargs):
        """
         s = Store()

         df = s(
            "What KT-2011 orders and what do you know about this body and any contracts to be careful of",
            ["queues.Body", "queues.Style", "queues.Order", "coda.resonance-contracts"],
         )
        df
        """

        return self.search_many(question, stores, **kwargs)


@ray.remote
def f(question, store):
    """
    this runs a search
    the vector search is slow because it is
    the columnar store is slow because use an LLM in the loop
    so running in parallel allows linear search and condensation of results
    """
    print("querying a store...")
    # todo think about faster load and sharing the store thing (db connections)
    return Store()[store](question)


@serve.deployment()
class QueryService:
    """
    Allow for distributed search of stores
    """

    def __init__(self):
        pass

    async def __call__(self, request):
        question = request.query_params["question"]

        store_names = COL_STORES + VEC_STORES
        store_names = request.get("stores") or store_names
        res.utils.logger.info(f"{question}, {store_names}")
        futures = [f.remote(question, s) for s in store_names]
        response = ray.get(futures)
        # what each store said
        response = dict(zip(store_names, response))

        # prune on distance overall - do so in each partial result but leave the indexes that were queried

        return response


app = QueryService.bind()
# test locally
# serve run service:app
# serve build service:app -o config.yaml
# serve deploy config.yaml
