from typing import Any
import pandas as pd
import res
from res.utils.env import RES_DATA_BUCKET
from tqdm import tqdm
import traceback
from res.utils.secrets import secrets


###
##  benchmarking model
##  for any entity get its attributes and then compare it across dimensions. for example the same piece in different sizes or colors  (aggregated to the body or style )
###
def get_table(table_name, namespace):
    table_path = f"s3://{RES_DATA_BUCKET}/data-lake/rmm/{namespace}/{table_name}/partition_0.parquet"
    return table_path


def load_data(data, table_name, namespace):
    s3 = res.connectors.load("s3")
    table_uri = get_table(table_name, namespace=namespace)
    res.utils.logger.debug(f"table {table_uri}")
    s3.write(table_uri, data)
    res.utils.logger.info(f"saved data to {table_uri}")


def get_default_llm():
    from langchain.llms import OpenAI

    secrets.get_secret("OPENAI_API_KEY")

    model_name = "gpt-4"
    return OpenAI(model_name=model_name, temperature=0.0)


def explain_data(
    df,
    min_cluster_size=50,
    sample_size=30,
    # match_terms=["is_", "_percentile"],
    model_name="gpt-4",  # -0613
    prompt_prefix=None,
    # dim_reduction_args
    # add the prompt as args,
    # specify max clusters and iterate with larger min clusters
):
    import hdbscan
    from tqdm import tqdm
    from langchain.llms import OpenAI
    import numpy as np

    """
    df is a dataframe properly prepared
     e.g. with a key and normalized and binary columns

    min_cluster_size: hdbascan see: https://hdbscan.readthedocs.io/en/latest/how_hdbscan_works.html
    sample_size: sub sample the class. value is small enough to fit in a token limit

    TODO:
    - option to skip the -1 cluster
    -
    """

    prefix = (
        prompt_prefix
        or """
            You will be given a dataset with boolean and rank columns and a class name called "cid" describing ecommerce products. 
            It describes instances of healings which are bad because something was sick and needed repair.
            It describes low and high values and high values are generally bad.
            List the interesting patterns in the data referring to the particular class.
            If columns are constant say so but emphasize any other low entropy columns that are not constant.
            At the end, given an overall summary of what you can say about the products in this class.
    """
    )
    # we only deal in numerics
    XT = df.select_dtypes(include=np.number).copy()
    for col in XT.columns:
        XT[col] = XT[col].rank(pct=True)
        XT[col] -= XT[col].min()
    # small token
    # round
    XT = XT.round(1)
    # remove correlated features

    # remove correlated stuff
    corr_matrix = XT.corr().abs()
    upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
    to_drop = [column for column in upper.columns if any(upper[column] > 0.95)]
    XT = XT.drop(to_drop, axis=1)

    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size, gen_min_span_tree=True
    )

    m = clusterer.fit(XT.dropna())

    XT["cid"] = m.labels_
    # return XT

    res.utils.logger.info(
        f"{XT['cid'].unique()} cluster ids in data of shape {XT.shape}"
    )
    llm = OpenAI(model_name=model_name, temperature=0.0)
    answers = []
    for i in tqdm(XT["cid"].unique()):
        sample = XT[XT["cid"] == i].sample(sample_size)
        res.utils.logger.info(
            f"\n<<<Asking the LLM for a summary for cluster indexed {i}. its slow....>>>\n"
        )
        ans = llm(
            f"""{prefix}
                Data: {sample.to_dict('records')}
            """
        )
        res.utils.logger.info(ans)
        answers.append(ans)

    res.utils.logger.info(
        "Generating overall summary will provide result as (Summary, Summaries, clusters)"
    )
    summary = llm(
        f"""The data provide summaries of different data classes.
            Please give an overall summary of the different classes,
            describing any differences and similarities between them.
            Mention which classes have the most problems recently and overall.
            Comment specifically on the low entropy features which may characterize these classes.
            Provide a structured answer in HTML format.
            Summaries:
            {answers}
        """
    )

    # we will need to return a result with answers, classes and
    return summary, answers, list(XT["cid"])

    # from IPython.display import HTML


class ResilientResonanceMap:
    """
    The RRM is a pattern for adding lookup data for all entities
    - styles
    - bodies
    - orders
    - make orders
    - rolls


    This can be used to easily get the context of each node
    Also managed in this system are external state tables and vector stores via data lake
    - duckdb is used to query tables in the map
    - lancedb is used to store embeddings
    - the RRM is trace aware in that we add system attributes for searching and linking

    """

    def __init__(
        self, entity_type, namespace, key_parser=None, text_factory=None, **kwargs
    ):
        self._cache = res.connectors.load("redis")["RRM"][f"{namespace}.{entity_type}"]
        # determines factories
        self._entity_name = entity_type
        self._namespace = namespace
        self._text_factory = lambda attributes: str(attributes)
        if text_factory is not None:
            self._text_factory = text_factory
        self._key_parser = lambda k: k
        if key_parser is not None:
            self._key_parser = key_parser
        self._columns = []
        # add anything to context - be careful what we put in here
        # may make this more explicit in future
        self._added_context = kwargs
        # there can be some lag in the below - we can lazy load this in a singleton or whatever makes sense in practice
        self._table = get_table(entity_type, namespace)
        self._inspect_sample_row()
        self._llm = get_default_llm()

        # TODO
        # 1 load enums on columns below certain size into context - for example queue state names -> this creates fuzy tolerance when user asks questions about states
        # 2

    def _inspect_sample_row(self):
        try:
            duck = res.connectors.load("duckdb")
            self._columns = list(
                duck.execute(f""" SELECT * FROM '{self._table}' """).columns
            )
        except:
            pass

    def __getitem__(self, key):
        key = self._key_parser(key)
        res.utils.logger.debug(key)
        return self._cache[key]

    def import_entities(self, data, key_field, map_fields=None):
        """
        pass a dataframe of objects and specify the key field to use to add them to the map
        The dataframe will be stored in parquet on s3 and the items will be added into the dictionary with a subset of fields
        """
        load_data(data, table_name=self._entity_name, namespace=self._namespace)
        res.utils.logger.debug(f"Loading key store using key {key_field}")
        for record in tqdm(data.to_dict("records")):
            # to str so it is not ambiguous what the key is
            key = str(record.get(key_field))
            self.put(key, record)

        self._inspect_sample_row()

    def load_entities(self):
        return self.select_from_data_lake_where(None)

    def agent_describe_table(
        self, enum_values=None, include_samples=False, return_type=str
    ):
        """
        A tool to ask the agent what it thinks are in the columns
        use this to consider new column names that will not be misleading or ambiguous
        """
        prompt = f"For the following columns, based on their names, tell me what data types you expect them to be and give a short description of the purpose of the column that you. expect. columns are {self._columns}. Provide your answer as a HTML table"
        if include_samples:
            prompt = f"""For the following columns, based on their names, tell me what data types you expect them to be and give a short description of the purpose of the column that you expect in html tabular format.
            The columns are {self._columns}.
            Additionally using the states and data table tool, for each column, add the ratio of distinct values per column to the overall length of the table. If there are less than 10 unique values in any given column, assume they are enum types and list ALL of the unique values for that column.
             
            """
        agent = self.as_agent()
        return return_type(agent(prompt)["output"])

    def select_from_data_lake_where(self, where_clause, columns=None):
        duck = res.connectors.load("duckdb")
        if where_clause is None:
            return duck.execute(f""" SELECT * FROM '{self._table}'  """)

        return duck.execute(f""" SELECT * FROM '{self._table}' where {where_clause} """)

    def put(self, key, attributes):
        """
        attributes should include system
        - typing
        - open telemetry tracing
        - dates

        #we want the last queue movement as well as the created at
        #we want any defects on the item
        #we want to know the done state on all items is_done
        #if defects we saw is_blocked
        """
        d = {"text": self._text_factory(attributes), "data": attributes}
        # add to the redis cache
        self._cache.put(key, d)

    def route_key(self, keys, **kwargs):
        from ast import literal_eval

        if "entities" in kwargs:
            keys = kwargs.get("entities")
        if "entities" in keys:
            try:
                keys = literal_eval(keys)["entities"]
            except:
                pass

        keys = keys.split(",")

        # we can override how we interpret the keys that are passed in

        return {k.strip(): self.get_text(k) for k in keys}

    def get_text(self, key):
        res.utils.logger.debug(f"key fetch {key}")
        attributes = self[key]
        if not attributes:
            return None
        attributes = attributes["data"]
        return self._text_factory(attributes)

    def as_agent(self, prompt_prefix=None, added_context=None, **kwargs):
        """
        you can ask questions directly of it
        """
        from langchain.agents import ZeroShotAgent, Tool, AgentExecutor
        from langchain import LLMChain
        from langchain.chat_models import ChatOpenAI

        secrets.get_secret("OPENAI_API_KEY")

        # note:
        # we can add some general rules like
        # If you are asked about orders you can use the sku as an identifier. this is needed because the queries that come back are undirected without that e.g. * results so this helps to refine
        prefix = (
            prompt_prefix
            or f"""Answer the question in the context of any entities you observe in the question or in the context.  
        If you cannot find information in the tool, try passing the entities in the question to the entity resolver tool.
        Use the stats and data tool to get specific entities and then use the entity resolver tool to find out information about those entities by passing a comma separated list of all entities to the entity resolver tool.
        Note any additional context: {added_context}
        You have access to the following tools:"""
        )

        suffix = """Begin! Give terse answers.
        Question: {input}
        {agent_scratchpad}"""

        # other things you can try" rather than terse ask: Give answers in a html table format
        # from IPython.display import HTML
        # HTML(table)

        tools = self.as_toolset()

        prompt = ZeroShotAgent.create_prompt(
            tools,
            prefix=prefix,
            suffix=suffix,
            input_variables=["input", "agent_scratchpad"],
        )

        llm_chain = LLMChain(
            llm=ChatOpenAI(model_name="gpt-4", temperature=0.0), prompt=prompt
        )
        tool_names = [tool.name for tool in tools]
        agent = ZeroShotAgent(llm_chain=llm_chain, allowed_tools=tool_names)
        agent_executor = AgentExecutor.from_agent_and_tools(
            agent=agent, tools=tools, verbose=True
        )
        return agent_executor

    def as_toolset(self, **kwargs):
        """
        a tool for taking structure or unstructured input and using the query or maps to find answers
        #some kwargs may look like strings of dictionaries with which we can update the kwargs
        """
        from langchain.tools import Tool

        return [
            Tool(
                name="EntityResolutionTool",
                func=self.route_key,
                description="Useful when you want to resolve entity types, get information about entities or check that your answer refers to the correct entity types",
            ),
            Tool(
                name="Stats and data table tool",
                # self is callable
                func=self,
                description="useful when you want to get statistics about entities, states of entities etc.",
            ),
        ]

    def __call__(self, question):
        return self._run(question)

    # 1. Use these synonyms if you need to: apply_color_flow_status values are also called subnodes
    def _run(self, question):
        q = f"""For a table called TABLE with the {self._columns}, give me an sql query that does not rename columns for duckdb that answers the question {question} and using the following rules;
        1. Always use the Stats and data table tool if you are not sure what tool to use or use the entity resolution tool if you are expect specification about an identity or identifier
        2. Only return the query.
        3. Do not rename any columns in select clause of the query unless they are included in the aliases. 
        4. Do not use any column names not in the list of columns i gave you but use aliases to map the users question to columns
        5. If you dont know, tell me what are the most likely columns to use in the where clause"""

        duck = res.connectors.load("duckdb")
        query = self._llm(q)
        query = query.replace("TABLE", f"'{self._table}'")
        res.utils.logger.debug(query)
        try:
            # option to break out of this happens
            # if " as " in query:
            #     raise Exception("query variable rename not allowed")
            df = duck.execute(query)
        except:
            res.utils.logger.warn(
                f"The query {query} does not work for duck db {traceback.format_exc()}"
            )
            return pd.DataFrame()
        # hash columns is like adding noise
        # df.columns = [res.utils.res_hash() for c in df.columns]
        return df  # .to_html()

    def _arun(self, key):
        raise NotImplementedError("This tool does not support async")


def get_rrm_tools():
    """
    temp quick script to generate the RRM as a tool set
    """
    from langchain.tools import BaseTool, Tool

    class OrderTool(BaseTool):
        name = "Order questions tool"

        description = "use this tool when you need answer questions about orders"

        def _run(self, question):
            r = ResilientResonanceMap("make", "production_requests")
            return r(question)

        def _arun(self, key):
            raise NotImplementedError("This tool does not support async")

    class EntityResolverTool(BaseTool):
        name = "Entity Resolver"
        description = "use this tool when you need to resolve entities"

        def _run(self, keys, **kwargs):
            from ast import literal_eval

            def routed(k):
                k = k.strip()

                print(k)
                try:
                    if len(k.split(" ")) == 4:
                        # temp
                        sku_part = f" ".join([s for s in k.split(" ")[:3]])
                        k = sku_part
                        r = ResilientResonanceMap("meta", "styles")
                        return r._cache[k]["text"]
                    if len(k.split(" ")) == 3:
                        r = ResilientResonanceMap("meta", "styles")
                        return r._cache[k]["text"]
                    if len(k.split(" ")) == 1 and "-" in k:
                        r = ResilientResonanceMap("meta", "bodies")
                        return r._cache[k]["text"]
                    else:
                        r = ResilientResonanceMap("make", "production_requests")
                        return r._cache[k]["text"]
                except:
                    # no key lookup response
                    return None

            # some agent dialect faff
            print(keys, kwargs)
            if "entities" in kwargs:
                keys = kwargs.get("entities")
            if "entities" in keys:
                try:
                    keys = literal_eval(keys)["entities"]
                except:
                    pass
            print(keys, kwargs)
            keys = keys.split(",")
            return {k.strip(): routed(k) for k in keys}

        def _arun(self, key):
            raise NotImplementedError("This tool does not support async")

    tools = [
        Tool(
            name="EntityResolutionTool",
            func=EntityResolverTool()._run,
            description="Useful when you want to resolve entity types, get information about entities of check that your answer refers to the correct entity types",
        ),
        Tool(
            name="Orders",
            func=OrderTool()._run,
            description="useful when you want to get statistics about orders, states of orders or lookup skus, body codes and order numbers",
        ),
    ]

    return tools


def example_reasonable_agent():
    from langchain.agents import ZeroShotAgent, Tool, AgentExecutor
    from langchain import LLMChain
    from langchain.chat_models import ChatOpenAI

    tools = get_rrm_tools()

    # note:
    # we can add some general rules like
    # If you are asked about orders you can use the sku as an identifier. this is needed because the queries that come back are undirected without that e.g. * results so this hepls to refine
    prefix = """Answer the question about entities. 
    You will be asked a question about orders. 
    If you are asked about orders you can use the sku as an identifier.
    If you cannot find information in the tool, try passing the entities in the question to the entity resolver tool.
    Use the order tool to get specific entities and then use the entity resolver tool to find out information about those entities by passing a comma separated list of all entities to the entity resolver tool.
    You have access to the following tools:"""

    suffix = """Begin! Give terse answers.
    Question: {input}
    {agent_scratchpad}"""

    # other things you can try" rather than terse ask: Give answers in a html table format
    # from IPython.display import HTML
    # HTML(table)

    prompt = ZeroShotAgent.create_prompt(
        tools,
        prefix=prefix,
        suffix=suffix,
        input_variables=["input", "agent_scratchpad"],
    )

    llm_chain = LLMChain(
        llm=ChatOpenAI(model_name="gpt-4", temperature=0.0), prompt=prompt
    )
    tool_names = [tool.name for tool in tools]
    agent = ZeroShotAgent(llm_chain=llm_chain, allowed_tools=tool_names)
    agent_executor = AgentExecutor.from_agent_and_tools(
        agent=agent, tools=tools, verbose=True
    )

    # call the run method with your question
    # e.g. What is the body version and also the body name of the two most recently cancelled order's skus?

    # agent_executor.run("What is the body version and body name of the most recently cancelled order's sku?")
    # agent_executor.run("What is the body version and also the body name of the two most recently cancelled order's skus?")
    # agent_executor.run("What is the body version, material and also the body name of the two most recently cancelled order's skus?")
    # agent_executor.run("What is the body version, material and also the body name of the two most recently cancelled orders?")

    # this works because we preload the cache with things like this for 4-part skus r._cache.put('TK-6093 CHRST LGACQS', {'text' : f'the entity type of {sku} is a SKU. The body associated with this sku has body version 2. A SKU is made of four components which are Body TK-6093 followed by Material, Color and Size' })
    # OR r._cache.put('TK-6093 CHRST LGACQS', {'text' : f'the entity type of {sku} is a SKU. The body associated with this sku has body version 2. A SKU is made of four components which are "Body Code"  followed by "Material", "Color" and "Size"' })
    return agent_executor
