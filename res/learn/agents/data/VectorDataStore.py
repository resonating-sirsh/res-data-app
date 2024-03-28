from llama_index.langchain_helpers.agents import IndexToolConfig, LlamaIndexTool


from langchain.agents import Tool

from langchain.chat_models import ChatOpenAI

# for the vectorstore too
from langchain.vectorstores import LanceDB
from langchain.document_loaders import DataFrameLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings import OpenAIEmbeddings
from langchain.chains import RetrievalQA
from schemas.pydantic.common import FlowApiAgentMemoryModel
from typing import List

import pandas as pd
import res


class VectorDataStore:
    """
    Vector store for infesting and query data
    can be used as a tool

    from res.learn.agents.data.VectorDataStore import VectorDataStore
    store = VectorDataStore(namespace='test', entity_name='test')
    tool = store.as_tool()
    tool.run("what is your question....")

    #data = store.load()

    """

    def __init__(
        self,
        namespace,
        entity_name,
    ):
        self._entity_name = entity_name
        self._namespace = namespace
        self._table_name = f"{namespace}_{entity_name}"

    def add(self, records: List[FlowApiAgentMemoryModel]):
        """
        loads data into the vector store if there is any big text in there
        """
        records = [r.large_text_dict() for r in records]
        records = [r for r in records if len(r)]
        vector_data = pd.DataFrame(records)
        if len(vector_data):
            res.utils.logger.info("Updating vector data")
            if len(vector_data):
                ingest_text_data_store(
                    vector_data,
                    entity_name=self._entity_name,
                    namespace=self._namespace,
                )
        return vector_data

    def load(self):
        db = res.connectors.load("lancedb").get_db()
        return db.open_table(self._table_name).to_pandas()

    def as_tool(self):
        return get_text_tool(entity_name=self._entity_name, namespace=self._namespace)


def ingest_text_data_store(
    df,
    entity_name,
    namespace,
    text_column="text",
    id_column="id",
    delete_existing_ids=True,
):
    """
    Our interface is just a dataframe with text and id column
    the id column is used to upsert /remove dups - which is done of delete_existing_ids option is true (default)

    the namespace.entity_name is used to name the table

    import pandas as pd
    df = pd.DataFrame([[1, "Sirsh looks body KT-3030 very much"],
                [2, "Frank really does not like body KT-3030"]],
                columns=['id', 'text']
                )
    table, _ = load_text_data_store(df, entity_name='body', namespace='meta')
    table.to_pandas()

    """
    if df is None:
        # some dummy default while i figure out the lance interface
        df = pd.DataFrame([[-1, "test"]], columns=["id", "text"])

    # we remove anything that does not have a valid text but we should warn for ids
    df = df.dropna(subset=["text"])

    text_column = "text"
    table_name = f"{namespace}_{entity_name}"
    embeddings = OpenAIEmbeddings()

    db = res.connectors.load("lancedb").get_db()
    df[id_column] = df[id_column].map(str)

    try:
        table = db.open_table(table_name)
        df["vector"] = df[text_column].map(embeddings.embed_query)
        in_list = ",".join([f'"{_id}"' for _id in df[id_column]])
        if delete_existing_ids:
            table.delete(f"{id_column} IN ({in_list})")
        exists_fields = table.schema.names
        for f in exists_fields:
            if f not in df.columns:
                df[f] = None

        table.add(df)
    except FileNotFoundError as fex:
        res.utils.logger.info(f"create a new table because of {fex}")
        df["vector"] = df[text_column].map(embeddings.embed_query)
        table = db.create_table(table_name, data=df, mode="overwrite")
    return table, df


def get_text_tool(entity_name, namespace, text_column="text"):
    """
    This tool first loads a lance db table and then creates a QA tool over it

    db = lancedb.connect(LANCE_ROOT).open_table('meta_bodies2')
    db.to_pandas()

    """
    embeddings = OpenAIEmbeddings()
    table, df = ingest_text_data_store(
        None, entity_name=entity_name, namespace=namespace, text_column=text_column
    )
    loader = DataFrameLoader(df, page_content_column=text_column)
    documents = RecursiveCharacterTextSplitter().split_documents(loader.load())

    docsearch = LanceDB.from_documents(documents, embeddings, connection=table)
    qa = RetrievalQA.from_chain_type(
        llm=ChatOpenAI(model_name="gpt-4", temperature=0.0),
        chain_type="stuff",
        retriever=docsearch.as_retriever(),
    )

    return Tool(
        name=f"Further details tool relate to {entity_name} entities",
        func=qa.run,
        description=f"""If and only if the other tools return no results, use this tool to get extra information about any {entity_name} entity that you are asked about.  
            Do not pass identifiers and codes to this tool. Only pass proper nouns and questions in full sentences""",
    )


def tool_from_index(
    index,
    name="Vector Index",
    description="useful for when you want to answer queries about ONE platform",
):
    """
    Simple wrapper example around an index like a slack index to build a tool with some hard coded settings for now
    """
    tool_config = IndexToolConfig(
        index=index,
        name=name,
        description=description,
        index_query_kwargs={"similarity_top_k": 3},
        tool_kwargs={"return_direct": True},
    )

    tool = LlamaIndexTool.from_tool_config(tool_config)
    return tool
    # tool.run("Why are we using PLT files?")
