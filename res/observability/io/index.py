import res
import json
import traceback
from res.observability.io import VectorDataStore, AbstractStore, typing
from res.observability.entity import FunctionDescriptionVectorStoreEntry, FunctionTypes
from stringcase import titlecase
from res.observability.dataops import parse_fenced_code_blocks
from res.observability.dataops import FunctionDescription


def get_function_index_store():
    Model = FunctionDescriptionVectorStoreEntry.create_model(name="FunctionsIndex")
    return VectorDataStore(Model)


def update_function_index(
    store: AbstractStore, summary: str, short_description: str = None
):
    """
    experimental - we can add an index over functions and stores
    """

    short_description = f"Search for {store.entity_namespace}.{store.entity_name} - you can use this to search for things related to {titlecase(store.entity_name)}\n{short_description}"
    summary = f"This is a data store search function for searching entities of type.\n{short_description or ''}\n{summary}"

    # short description is prompt loaded into the LLM but we search by the broader search
    fnd = store.as_function_description(
        short_description,
        # adding an alias
        name=f"run_search_for_{store.entity_namespace}_{store.entity_name}",
    )

    res.utils.logger.debug(f"{fnd.name, fnd.description}")

    Model = FunctionDescriptionVectorStoreEntry.create_model(
        name="FunctionsIndex", function_namespace=store.entity_namespace
    )
    fstore = VectorDataStore(Model)

    # we will search on the summary - the names should be categories
    id = f"{store.entity_namespace}.{store.entity_name}.{fnd.name}"
    record = Model(
        id=id,
        name=fnd.name,
        text=summary,
        function_class=(
            FunctionTypes.VectorSearch.value
            if isinstance(store, VectorDataStore)
            else FunctionTypes.ColumnarQuantitativeSearch.value
        ),
        function_description=json.dumps(fnd.dict()),
    )

    res.utils.logger.info(f"Indexed {short_description} - id is {id}")

    # in future we could add N different functions per store source for different scenarios e.g. time based, "summary" category filter is also a thing

    fstore.add(record)

    return store


def _summary_helper(texts):
    """
    Pass example texts and get summary and keywords and entities
    """
    from res.learn.agents import InterpreterAgent
    from res.learn.agents.InterpreterAgent import DEFAULT_MODEL

    agent = InterpreterAgent()

    act = f"""Please provide a summary of the text in a couple of paragraphs in a json format e.g. {{'summary':'<SUMMARY HERE>'}}
        Also 
        - list any Full Names mentioned (you should simply omit partial names) and 
        - list of non-name key categories or topics that describe the summary using recognizable ontological categories
        You should provide the response in json format with attributes `summary`, 'entity_refs`, 'keywords'
        TEXT:
        ```
        {texts}
        ```
        """

    try:
        data = agent.ask(act, response_format=None, model=DEFAULT_MODEL)
    except:
        # temp
        return ""

    try:
        return json.loads(data)
    except:
        return parse_fenced_code_blocks(data, select_type="json")[0]


def add_cluster_summaries(
    store: VectorDataStore, sample_size=7, plot_clusters=True, **kwargs
):
    """
    For a given store, we cluster using parameters and then summarize N neighbours and re-save a node ad d=-1 with type == summary
    we update the story to filter by level or type so that we can probe a store
    the summary should embed graph edges to related texts

    extend the schema used in the store to have labels (keywords) and entity refs lust as columns - when asking for a summary use the structure; summary, entity_ref keywords
    """

    import numpy as np
    import umap
    import umap.plot
    import polars as pl
    import hdbscan

    from res.learn.agents import InterpreterAgent

    agent = InterpreterAgent()

    # import plotly.express as px

    df = pl.DataFrame(store.load()[["name", "text", "doc_id", "vector", "id"]])

    v = np.stack(df["vector"].to_list())
    res.utils.logger.debug(f"Fitting data...")
    emb = pl.DataFrame(
        umap.UMAP().fit(v).embedding_, schema={"x": pl.Float32, "y": pl.Float32()}
    )

    try:
        clusterer = hdbscan.HDBSCAN(min_cluster_size=10, gen_min_span_tree=True)
    except:
        # in case of small clusters
        clusterer = hdbscan.HDBSCAN(min_cluster_size=3, gen_min_span_tree=True)

    c = clusterer.fit_predict(emb[["x", "y"]].to_numpy())
    emb = emb.with_columns(pl.Series(name="cluster", values=c))
    df = df.hstack(emb)

    Model = store.get_data_model(True)
    cluster_ids = [cid for cid in df["cluster"].unique() if cid != -1]
    records = []
    res.utils.logger.debug(
        f"Adding summaries to store for clusters length {len(cluster_ids)}"
    )
    # limit clusters in no particular order

    for c in cluster_ids[:20]:
        texts = df.filter(pl.col("cluster") == c).sample(sample_size)["text"].to_list()
        texts = "\n".join([t for t in texts if t])
        res.utils.logger.debug(f"Text of length {len(texts)}")
        # temp

        try:
            # here we take a summary of the N delegate samples - one per cluster
            cluster_summary = agent.summarize(
                "Provide a two paragraph summary of the ideas in the text", data=texts
            )
        except:
            continue

        res.utils.logger.debug(cluster_summary)
        # create a record from the summary
        record = Model(
            name=f"summary_{c}",
            depth=-1,
            text=cluster_summary.get("summarized_response"),
            doc_id="cluster_summaries",
        )
        records.append(record)

    super_summary = ""
    try:
        super_summary = _summary_helper(f"\n".join([r.text for r in records]))

    except:
        res.utils.logger.warn(f"failed to summary overall: {traceback.format_exc()}")

    try:
        res.utils.logger.debug(f"Adding {len(records)} samples")
        store.add(records)
    except:
        res.utils.logger.warn(
            f"Failed to add the records - you are probably using an incorrect schema"
        )
    store.set_summary(super_summary)

    if plot_clusters:
        import plotly.express as px

        px.scatter(x=df["x"], y=df["y"], color=df["cluster"])
    return store
    # find centers / sample from the clusters


def index_stores(
    skip_exists=False,
    filter_names=None,
    filter_namespaces=None,
    filter_types=None,
    start_index=0,
):
    from res.observability.io import list_stores, open_store

    for store_info in list_stores()[start_index:]:
        try:
            print(store_info)
            if filter_names and store_info["name"] not in filter_names:
                continue
            if filter_types and store_info["type"] not in filter_types:
                continue
            if filter_namespaces and store_info["namespace"] not in filter_namespaces:
                continue
            store: AbstractStore = open_store(**store_info)
            if store_info["type"] == "vector-store":
                store = add_cluster_summaries(store)
            # experimental - use enums because they
            elif store_info["type"] == "columnar-store":
                store._summary = {
                    "summary": f"This is a columnar status and data store of type {store_info['type']} for entity {store._entity_namespace}.{store._entity_name} that can be used to search details about entities. It has values such as {store._enums}"
                }
            else:
                store._summary = {
                    "summary": f"This is a store of type {store['type']} that can be used to search details about entities"
                }
            res.utils.logger.debug(f"<<<<adding summary {store._summary}>>>>> ")
            update_function_index(store, summary=json.dumps(store._summary))
        except Exception as ex:
            res.utils.logger.warn(f"Failed {traceback.format_exc()}")


def search_and_load_functions(queries: typing.List[str], limit: int = 3):
    """search vector store for functions

    Args:
        queries: queries to search for functions that might help
        limit: number of results to return

    """
    from res.observability.entity import FunctionDescriptionVectorStoreEntry
    from res.observability.io import VectorDataStore
    from res.observability.dataops import FunctionDescription

    Model = FunctionDescriptionVectorStoreEntry.create_model(
        name="FunctionsIndex", namespace="entity"
    )
    st = VectorDataStore(Model)
    results = st(
        queries=queries,
        columns=["id", "text", "doc_id", "function_description"],
    )

    return [
        FunctionDescription.restore(d["function_description"], weight=d["_distance"])
        for d in results
    ][:limit]


def index_api_function(
    endpoint,
    schema_file_json=None,
    verb="get",
    api_root="https://data.resmagic.io",
    plan=False,
) -> FunctionDescription:
    """

    using an openapi spec e.g 'https://data.resmagic.io/meta-one/openapi.json'
    choose endpoints and verbs to index. eg. /meta-one/meta-one/active_style_piece_info
    if the schema file is null it will be inferred as root/<head>/openapi.json. <head> is e.g. /<meta-one>/..
    set plan to False to just return the function description or otherwise add it to the stored index for use
    """

    if schema_file_json is None:
        head = endpoint.lstrip("/").split("/")[0]
        schema_file_json = f"{api_root}/{head}/openapi.json"
        res.utils.logger.info(f"inferred schema file {schema_file_json}")
    fnd = FunctionDescription.from_openapi_json(
        schema_file_json, endpoint=endpoint, verb=verb
    )

    if plan:
        return fnd

    ns = endpoint.lstrip("/").split("/")[0]
    Model = FunctionDescriptionVectorStoreEntry.create_model(
        name="FunctionsIndex",
        # use the convention that the root of the end point is the app name ......
        function_namespace=ns,
    )
    fstore = VectorDataStore(Model)

    # we will search on the summary - the names should be categories
    id = f"api.GENERAL.{endpoint}_{verb}"
    record = Model(
        id=id,
        name=fnd.name,
        text=f"An API CALL for the endpoint {endpoint} verb={verb} - {fnd.description}",
        function_class="api",
        function_description=json.dumps(fnd.dict()),
    )

    fstore.add(record)

    return fnd


def index_data(data, name, namespace, key="name"):
    """

    illustrate the pattern for creating a store from data and indexing it
    key is name by default but should be set

    """
    from res.observability.entity import AbstractEntity
    from res.observability.io import ColumnarDataStore
    from res.observability.io.index import index_stores

    # create a model from data
    Model = AbstractEntity.create_model_from_data(name, namespace=namespace, data=data)

    # create a store from model
    store = ColumnarDataStore(Model)

    # load the data into the model records
    records = [Model(**r) for r in data.to_dict("records")]

    # add the data to the store
    store.add(records, key_field=key, mode="overwrite")

    # index the story: todo add some more descriptions here
    index_stores(filter_names=[name])

    # you can now use the store
    return store
