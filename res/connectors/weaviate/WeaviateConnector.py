# rough work to start working with weaviate
# https://github.com/weaviate/weaviate-examples/blob/main/exploring-multi2vec-clip-with-Python-and-flask/data.py
import res
import tempfile
import uuid
import io
import os

try:
    import weaviate
except:
    pass


def define_schema_snippet(client):
    class_obj = {
        "class": "GarmentPiecesColorThumb",
        "description": "Pieces of agrments",
        "properties": [
            {"dataType": ["blob"], "description": "Image", "name": "image"},
            {"dataType": ["string"], "description": "", "name": "path"},
        ],
        "vectorIndexType": "hnsw",
        "moduleConfig": {"img2vec-neural": {"imageFields": ["image"]}},
        "vectorizer": "img2vec-neural",
    }

    return client.schema.create_class(class_obj)


class WeaviateConnectorClient:
    def __init__(self):
        # temp
        self._client = weaviate.Client(
            url=os.environ.get(
                "WEAVIATE_URL", "http://localhost:4280"
            ),  # Replace with your endpoint
        )

    def index_images(self, uris, class_name="GarmentPiecesColorThumb"):
        s3 = res.connectors.load("s3")
        # some progress bar stuff
        res.utils.logger.info(f"Indexing {len(uris)} files")
        for file_name in uris:
            file_path = file_name
            with s3.file_object(file_path) as f:
                reader = io.BufferedReader(io.BytesIO(f.read()))
                b64_string = weaviate.util.image_encoder_b64(reader)

            r = self._client.data_object.create(
                {
                    "path": file_name,
                    "image": b64_string,
                },
                class_name,
                str(uuid.uuid4()),
            )
        res.utils.logger.info(f"Done")

    def search_images(self, uri, class_name="GarmentPiecesColorThumb"):
        s3 = res.connectors.load("s3")
        with s3.file_object(uri, mode="rb") as sf:
            with tempfile.NamedTemporaryFile(suffix=".png", prefix="f", mode="wb") as f:
                f.write(sf.read())
                f.flush()
                near_image = {"image": f.name}
                query = (
                    self._client.query.get(class_name, ["path"])
                    .with_near_image(near_image)
                    .with_limit(3)
                )
                res = query.do()

        return res["data"]["Get"][class_name]

    def similarity_search(self, query, classname, columns):
        """ """
        from langchain.vectorstores import Weaviate as LangChainWeaviate

        vs = LangChainWeaviate(self._client, classname, columns)
        return vs.similarity_search(query)

    def as_langchain_toolkit(
        self, classname, columns, llm, name=None, description=None
    ):
        """
        snippet for creating a toolkit from the vector store in context- need to generalize later

        """
        from langchain.agents.agent_toolkits import (
            create_vectorstore_agent,
            VectorStoreToolkit,
            VectorStoreInfo,
        )
        from langchain.vectorstores import Weaviate as LangChainWeaviate

        vs = LangChainWeaviate(self._client, classname, columns)
        name = name or f"{classname} store"
        description = description or f"Useful for getting {classname} details"
        vectorstore_info = VectorStoreInfo(
            name=name, description=description, vectorstore=vs
        )
        toolkit = VectorStoreToolkit(vectorstore_info=vectorstore_info)
        agent_executor = create_vectorstore_agent(
            llm=llm, toolkit=toolkit, verbose=True
        )

        # example where we can create something chat agent with memory from the toolkit
        # memory = ConversationBufferMemory(memory_key="chat_history")

        # agent_chain = create_llama_chat_agent(
        #     toolkit,
        #     llm,
        #     memory=memory,
        # )

        # agent_chain.run(input="Query about X")

        return agent_executor

    # def vectorestore_db_chain(self, chat_history=[]):
    #     from langchain.chains import ChatVectorDBChain
    #     oia = OpenAI(temperature=0.0)
    #     vs = LangChainWeaviate(self._client, classname, columns)

    #     qa = ChatVectorDBChain.from_llm(oia, vs)
    #     return qa({"question":query, "chat_history": []})

    # TODO show how we ingest with ai embeddings into vector stores and create agents of different types
