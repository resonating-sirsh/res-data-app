import os
import lancedb
import res
from res.utils import secrets_client

LANCE_ROOT = "s3://res-data-platform/lancedb/sirsh/tables"


def lance_connect(root=None):
    """
    This keys is basically for S3 access and we are specific about it
    """
    root = root or LANCE_ROOT

    if "AWS_ACCESS_KEY_ID" not in os.environ:
        aws_keys = secrets_client.get_secret("AWSKEY_DEFAULT")
        AWS_ACCESS_KEY_ID = aws_keys["ACCESS_KEY"]
        AWS_SECRET_ACCESS_KEY = aws_keys["SECRET_ACCESS_KEY"]
        os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
        os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
    return lancedb.connect(root, region=os.environ.get("AWS_DEFAULT_REGION"))


# https://lancedb.github.io/lancedb/basic/


class LanceDBConnector:
    def __init__(self, **kwargs):
        pass

    def __getitem__(self, key):
        return LanceDBTable(key)

    def get_db(self):
        return lance_connect()

    @staticmethod
    def get_embedding_vector(text):
        import openai

        def embed_func(c):
            rs = openai.Embedding.create(input=c, engine="text-embedding-ada-002")
            return [record["embedding"] for record in rs["data"]]

        return embed_func(text)[0]

    def embed_dataframe_for_langchain(
        self, df, table_name, text_column="text", add_id=False, return_qa_agent=True
    ):
        """
        experimental:
        the idea is we can pass any dataframe and map the text columns to embeddings and documents for use in langchain
        this is a useful pipeline method since anything can be converted to dataframe

        the id column should be user owned and unique and we should test for upserting records into the store based on ids

        documents (a langchain type) are returned with text and embeddings along with ids and scores - possibly the ids can be used to reference other things

        Assumptions for now on the df passed in
        - Unique id field e.g. res hash
        - no empty text fields
        - column called text exists
        - add a source uri or detail for the with-source retriever or do a mode that does not need it :> from langchain.chains import RetrievalQA
        ^we could soften some of these assumptions with some testing on how the interfaces work

        Example use case - indexing feedbacks - you could also index a slack conversation or something like that - provide the slack sources if we do

            import res

            airtable = res.connectors.load('airtable')
            df = airtable.get_table_data('appa7Sw0ML47cA8D1/tblORtfvTdeWZxPkR')
            dff = df[['Name','Summary']].reset_index().rename(columns={'Summary' :'text'}).dropna(subset=['text'])
            dff['source'] = df['record_id'].map(lambda x : f"https://airtable.com/appa7Sw0ML47cA8D1/tblORtfvTdeWZxPkR/{x} ")
            dff['id'] = dff['index'].map(lambda x : res.utils.res_hash())
            #how you would load lance
            lance = res.connectors.load('lancedb')
            qa = lance.embed_dataframe_for_langchain(dff, table_name='feedbacks')



        #just do a sim search
             docs = docsearch.similarity_search("What issues are we having with Rugby shirts?")

        #get table
            table.to_pandas()

        call this function and return a retriever;
        ask question with
            qa.run('Summarise all the cases that were having problems with trim')
            :> 'The cases with trim problems include: 1) trim tab showing empty, no trim mapped, and missing thread color; 2) missing thread information for request 10301277; 3) trims printed incorrectly in the waistband piece; and 4) missing piping trim for the ONE, not mapped on the trim details.'


        NOTE: im not sure what langchain method create a doc loader from an existing table so spoof with this with some assumptions about its suitability as a doc
            loader = DataFrameLoader(table.to_pandas()[:1], page_content_column="text")
            dummy_documents = RecursiveCharacterTextSplitter().split_documents(loader.load()) #loads documents and we split them
            docsearch = LanceDB.from_documents(dummy_documents, embeddings, connection=table)

        TODO: benchmark against other retrievers like
        # from langchain.vectorstores import FAISS
        # db = FAISS.from_documents(texts, embeddings)
        # # Init your retriever. Asking for just 1 document back
        # retriever = db.as_retriever()
        """
        from langchain.vectorstores import LanceDB
        from langchain.document_loaders import DataFrameLoader
        from langchain.text_splitter import RecursiveCharacterTextSplitter
        from langchain.embeddings import OpenAIEmbeddings
        from langchain.chains import RetrievalQAWithSourcesChain
        from langchain.chat_models import ChatOpenAI
        import lancedb

        # some dataframe that includes a text column
        loader = DataFrameLoader(df, page_content_column=text_column)
        documents = RecursiveCharacterTextSplitter().split_documents(loader.load())
        embeddings = OpenAIEmbeddings()

        # seems like something needs me to create a column called text -todo see if its an arg
        # im using this to check up front if i can create one row and then load the rest of the docs
        def probe(_df):
            d = dict(_df.iloc[0])
            d["vector"] = embeddings.embed_query(d[text_column])
            return d

        db = lance_connect()
        table = db.create_table(
            table_name, data=[probe(df.drop("index", 1))], mode="overwrite"
        )

        res.utils.logger.info(f"Building embeddings for {len(documents)} documents")
        docsearch = LanceDB.from_documents(documents, embeddings, connection=table)

        # this is returned here just to illustrate usage
        if return_qa_agent:
            qa = RetrievalQAWithSourcesChain.from_chain_type(
                llm=ChatOpenAI(model_name="gpt-4", temperature=0.0),
                chain_type="stuff",
                retriever=docsearch.as_retriever(),
            )
            # qa( "What issues are we having with Rugby shirts? Provide the entity identifiers in your answer" )
            return qa

        # or return the doc search / table can be loaded from lance any time

        return docsearch

    def _list_table_data(self):
        return list(res.connectors.load("s3").ls(LANCE_ROOT))

    def list_tables(self):
        l = self._list_table_data()
        return list(set([f.split(".")[0].split("/")[-1] for f in l]))


class LanceDBTable:
    def __init__(self, table, **kwargs):
        db = lance_connect()
        self._table = db.open_table(table)

    def table(self):
        return self._table

    def load(self):
        return self._table.to_pandas()

    def search(self, text, limit=5):
        return (
            self._table.search(LanceDBConnector.get_embedding_vector(text))
            .limit(limit)
            .to_df()
        )
