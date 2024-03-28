import pandas as pd
from langchain.agents import Tool
from langchain.chat_models import ChatOpenAI
import res
from res.utils.env import RES_DATA_BUCKET
from schemas.pydantic.common import FlowApiAgentMemoryModel


def parse_out_sql_and_try_clean(s):
    try:
        # probably a nicer utility for this
        if "```" in s:
            s = s.split("```")[1].replace("sql", "").strip("\n")
    except:
        pass

    try:
        # total hack
        return s.replace("CURRENT_DATE ", "CURRENT_DATE()")
    except:
        pass
    return s


def stats_tool_using_text_to_sql_for_df(
    namespace,
    table_name,
    enums=None,
    return_type="dict",
    build_enums=True,
    enum_threshold=50,
    limit_table_rows=50,
):
    """
    PASSING FULL SENTENCES TO THIS TOOL IS BEST UNLIKE THE ENTITY RES TOOL
    ENTITY RES TOOLS SHOULD PROVIDE A POINT TO THIS TOOL
    this tool internally builds an SQL query that asks a question and returns a dataframe with an answer
    that dataframe could be used by other tools
    columns and enums are probed to help answer questions

    TODO: check that the columns mean what we think they mean by best practice and add any helpful computed fields like booleans
    """
    table_path = f"s3://{RES_DATA_BUCKET}/data-lake/rmm/{namespace}/{table_name}/partition_0.parquet"
    duck = res.connectors.load("duckdb")
    res.utils.logger.info(f"inspecting {table_path}")
    df = duck.execute(f"select * from '{table_path}'")
    columns = list(df.columns)

    def try_unique(c):
        try:
            return len(df[c].unique())
        except:
            return enum_threshold

    enums = ""
    if build_enums:
        enum_types = [c for c in columns if try_unique(c) < enum_threshold]
        enums = {c: list(df[c].unique()) for c in df.columns if c in enum_types}

    # print(columns, enums)

    # assume this for now
    llm = ChatOpenAI(model_name="gpt-4", temperature=0.0)

    # wrap the question in the prompt - its the tools job to make a smart prompt
    # https://duckdb.org/docs/sql/query_syntax/select
    def ask(question):
        prompt = f"""For a table called TABLE with the {df.columns}, and the following column enum types {enums} ignore any columns asked that are not in this schema and give
              me a duckdb dialect sql query without any explanation that answers the question below. You should use the following observations about the Duck DB SQL dialect when working with dates;
              1. This SQL dialect has a CURRENT_DATE function to get current date 
              2. Do not ever use DATE() function in the where clause or in any other part of the query. Instead you can express a date by just writing DATE 2020-01-01 without parentheses.  
              3. date_part function can be used for date part questions
              Question: {question} """

        query = llm.predict(prompt)
        res.utils.logger.info(query)
        query = query.replace("TABLE", f"'{table_path}'")
        try:
            query = parse_out_sql_and_try_clean(query)
            # note that the data may have changed under the agent.
            data = res.connectors.load("duckdb").execute(query)
            if limit_table_rows:
                data = data[:limit_table_rows]
            if return_type == "dict":
                return data.to_dict("records")
            return data
        except Exception as ex:
            res.utils.logger.warn(f"duck cant cope {ex}")
            return pd.DataFrame()

    return Tool(
        name=f"Stats and data table tool relating to {namespace} {table_name}",
        func=ask,
        description=f"""Use this tool to answer questions about aggregates or statistics or to get sample values or lists of values relating to {namespace} {table_name}. 
        Do not select any values that are not in the provided list of columns. Provide full sentence questions to this tool. If 0 results are returned, do not trust this tool at all.""",
    )


def _merge_data(uri, data, key):
    """
    dedupe and merge on key
    """
    s3 = res.connectors.load("s3")

    if s3.exists(uri) and len(data):
        existing = s3.read(uri)
        res.utils.logger.info(
            f"Existing data with {len(existing)} rows - checking if we have anything new in {len(data)} incoming records using key {key}"
        )
        data = pd.concat([existing, data]).drop_duplicates(subset=[key], keep="last")
    res.utils.logger.info(
        f"Complete data with {len(data)} rows - writing to path {uri}"
    )
    s3.write(uri, data)
    return data


class ColumnarDataStore:
    """
    Load a datastore or use it as a too
    from res.learn.agents.data.ColumnarDataStore import ColumnarDataStore
    st = ColumnarDataStore(namespace='make',entity_name='one')
    data = st.load()
    st.as_tool()

    """

    def __init__(self, entity_name, namespace):
        self._entity_name = entity_name
        self._namespace = namespace
        self._table_path = f"s3://{RES_DATA_BUCKET}/data-lake/rmm/{namespace}/{entity_name}/partition_0.parquet"

    @property
    def entity_name(self):
        return self._entity_name

    @property
    def namespace(self):
        return self._namespace

    @property
    def table_path(self):
        return self._table_path

    def load(self):
        return res.connectors.load("s3").read(self._table_path)

    def as_tool(self, **options):
        return stats_tool_using_text_to_sql_for_df(
            self._namespace, self._entity_name, **options
        )

    def add(self, records: FlowApiAgentMemoryModel):
        """
        Add the fields configured on the pydantic type that are columnar - defaults all
        """
        records = [r.columnar_dict() for r in records]
        records = [r for r in records if len(r)]
        data = pd.DataFrame(records)
        if len(data):
            return _merge_data(self._table_path, data, key="id")
        return data
