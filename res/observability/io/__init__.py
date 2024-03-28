import os
from glob import glob
import res
from pathlib import Path
from functools import partial
import pyarrow as pa

EMBEDDINGS = {}


def get_embedding_provider(provider):
    if provider in EMBEDDINGS:
        return EMBEDDINGS[provider]
    else:
        if provider == "instruct":
            from InstructorEmbedding import INSTRUCTOR

            model = INSTRUCTOR("hkunlp/instructor-large")
            EMBEDDINGS[provider] = model
            return model.encode
        if provider == "clip":
            from lancedb.embeddings import EmbeddingFunctionRegistry

            registry = EmbeddingFunctionRegistry.get_instance()
            clip = registry.get("open-clip").create()

            def embed_uri(uri):
                image = res.connectors.load("s3").read_image(uri)
                return clip.compute_source_embeddings([image])[0]

            EMBEDDINGS[provider] = embed_uri
            return embed_uri


DEFAULT_HOME = "s3://res-data-platform/stores/"
STORE_ROOT = os.environ.get("RES_STORE_HOME", DEFAULT_HOME).rstrip("/")
VECTOR_STORE_ROOT_URI = f"{STORE_ROOT}/vector-store"
COLUMNAR_STORE_ROOT_URI = f"{STORE_ROOT}/columnar-store"


def get_agent():
    from res.learn.agents import InterpreterAgent

    return InterpreterAgent()


from .LanceDataTable import LanceDataTable
from .AbstractStore import AbstractStore
from .VectorDataStore import VectorDataStore
from .ColumnarDataStore import ColumnarDataStore
from .EntityDataStore import EntityDataStore
import json
import typing

STORES = {}


def list_stores(force=False):
    # simple cache
    if not force and "data" in STORES:
        return STORES["data"]

    from res.connectors import load
    import pandas as pd

    s3 = load("s3")

    def comp(f, store):
        f = f.replace(store, "").lstrip("/")
        # print(f)
        try:
            return {
                "name": f.split("/")[1].split(".")[0],
                "namespace": f.split("/")[0],
                "type": store.rstrip("/").split("/")[-1],
            }
        except:
            # temp
            return {}

    vector_stores = [
        comp(c, VECTOR_STORE_ROOT_URI)
        for c in s3.ls(VECTOR_STORE_ROOT_URI)
        if "instruct" not in c
    ]
    column_stores = [
        comp(c, COLUMNAR_STORE_ROOT_URI) for c in s3.ls(COLUMNAR_STORE_ROOT_URI)
    ]

    data = (
        pd.DataFrame(vector_stores + column_stores).drop_duplicates().to_dict("records")
    )

    STORES["data"] = data

    return data


def open_store(
    name: str, type: str, namespace: str = "default", embedding_provider="open-ai"
):
    """
    Convenience to load store by name. the interface is still being worked out
    """
    from res.observability.entity import (
        AbstractEntity,
        AbstractVectorStoreEntry,
    )

    # try to load the stores and check the type
    stores = list_stores()

    store_info = [
        s for s in stores if s["name"] == name and s["namespace"] == namespace
    ]
    if not store_info:
        # force just to be sure
        stores = list_stores(True)
        store_info = [
            s for s in stores if s["name"] == name and s["namespace"] == namespace
        ]
    if store_info:
        type = type or store_info[-1]["type"]

    store = VectorDataStore if type == "vector-store" else ColumnarDataStore

    # open-ai is the default for vector stores for now
    model = AbstractVectorStoreEntry if type == "vector-store" else AbstractEntity

    Model = model.create_model(name, namespace=namespace)
    store = store(Model)

    return store


def search_store(
    questions: typing.List[str],
    namespace: str,
    store_name: str,
    # enum over vector stores etc
    store_type: str = "vector-store",
    category: str = None,
    last_n_days: str = None,
):
    """
    the generic signature for calling a store search with one or more questions. Various context parameters can slice data by time or category
    a store is defined by type/namespace/name

    **Args**
        questions: one or more questions to ask of the store - usually full text questions unless otherwise specified,
        namespace: the namespace for the store required,
        store_name: the store name,
        # enum over vector stores etc
        store_type: the store type vector-store|columnar-store|entity-store
        category: a sub category in the store if relevant
        last_n_days: a simple time filter to restrict temporal relevance
    """
    pass


"""

polars helpers - refactor 
"""
import polars as pl
import s3fs
import pyarrow.dataset as ds


def read_dataset(uri) -> ds.Dataset:
    fs = None if uri[:5] != "s3://" else s3fs.S3FileSystem()
    # we choose to infer the format
    format = uri.split(".")[-1]
    return ds.dataset(uri, filesystem=fs, format=format)


def read(uri, lazy=False) -> pl.DataFrame:
    """
    read data to polar data
    """
    dataset = read_dataset(uri)
    if lazy:
        return pl.scan_pyarrow_dataset(dataset)

    return pl.from_arrow(dataset.to_table())


def write(uri, data: typing.Union[pl.DataFrame, typing.List[dict]], schema=None):
    """
    write data from polar data to format=parquet
    """

    def _get_writer(df, uri=None):
        """
        the writer is determined from the uri and defaults to parquet
        """
        # TODO generalize / assume parquet for now for our usecase
        return partial(df.write_parquet)

    if not isinstance(data, pl.DataFrame):
        # assume the data are dicts or pydantic objects
        # im using pyarrow which seems easier to control/test right now?
        # in particular the polars time inference on data fields some whack
        data = pl.DataFrame(pa.Table.from_pylist([r for r in data], schema=schema))

    fs = None if uri[:5] != "s3://" else s3fs.S3FileSystem()

    fn = _get_writer(data, uri)
    if fs:
        with fs.open(uri, "wb") as f:
            fn(f)
    else:
        # create director
        Path(uri).parent.mkdir(exist_ok=True, parents=True)
        fn(uri)

    # we would not always need to read back - its expensive but useful for dev
    return read_dataset(uri)


def merge(
    uri: str, data: typing.Union[pl.DataFrame, typing.List[dict]], key: str, schema=None
) -> ds.Dataset:
    """
    merge data from polar data using key

    """

    def read_if_exists(uri, **kwargs):
        # TODO: add some s3 clients stuff
        try:
            # fs = get_filesystem()
            # if fs and not fs.exists(uri):
            #     return None
            return read(uri=uri, **kwargs)
        except:
            return None

    existing = read_if_exists(uri)

    # TODO - we need to handle schema migration here at least for simple cases
    # for example if someone adds a new data field, we should just add it to the existing to support the concatenation

    if isinstance(data, list):
        # assume the data are dicts or pydantic objects
        # polars not as goof as pyarrow for this - more testing needed - types are sensitive
        # in particular the polars time inference on data fields some whack
        data = pl.DataFrame(pa.Table.from_pylist([r for r in data], schema=schema))

    if not isinstance(data, pl.DataFrame):
        raise Exception(
            "Only list of dicts and polar dataframes are supported - what did you pass in?"
        )
    if existing is not None:
        data = pl.concat([existing, data])

    write(uri, data.unique(subset=[key], keep="last"))

    return read_dataset(uri)
