import json
import res

from res.learn.agents.builder import (
    FunctionDescription,
    get_function_index_store,
    AgentConfig,
    ask,
)


class FunctionManager:
    def __init__(self):
        s3 = res.connectors.load("s3")
        # todo a smarter function lookup
        self._functions = s3.read(
            "s3://res-data-platform/ask-one/functions/lookup.feather"
        )
        self._cache = res.connectors.load("redis")["OBSERVE"]["FUNCTIONS"]

    @staticmethod
    def rebuild_cache():
        data = get_function_index_store().load()[
            ["name", "text", "function_class", "function_description"]
        ]
        _cache = res.connectors.load("redis")["OBSERVE"]["FUNCTIONS"]

        for record in data.to_dict("records"):
            _cache[record["name"]] = record["function_description"]
            # wse dont want the clunky description in the lookup - only in the KV cache
        res.connectors.load("s3").write(
            "s3://res-data-platform/ask-one/functions/lookup.feather",
            data.drop("function_description", 1),
        )
        return data

    @property
    def function_names(self):
        return list(self._functions["name"])

    def __getitem__(self, key):
        f = self._cache[key]
        if f:
            # always ensure the alias is what its mapped to ?? weird
            return FunctionDescription.restore(f, alias=key)
        res.utils.logger.warn(f"did not find {key} in the store")

    def function_search_agent_config(
        self, name, prompt, function_search_prompt, max_functions=7, namespace="default"
    ):
        return AgentConfig(
            name=name,
            namespace=namespace,
            prompt=prompt,
            functions=self.function_search(
                function_search_prompt, max_functions=max_functions
            ),
        )

    def function_search(self, question, max_functions=7):
        # Q = "I need some function to search the queue of bodies and for any bodies that have specific statuses , get their body nested by material. I would also like to see any conversations about them being discussed in the apply color queue channel "
        P = f"""
           Below are a map of functions that we have. There are different types of functions such as Vector Search, Columnar Search, API calls.
           - Vector Searches are usually over text stores like Slack or the Coda wiki and can provide general commentary. 
           - The columnar stores tend to me more structured and related to work queues in the Resonance Supply chain.
           - API calls are useful if answering specific queries where arguments have been supplied in the form of codes, ids or specific search parameters.
           
           Use this information to always return a selection of the most useful functions and the rating based on the users search criteria below.  
           
           
           **Search criteria for selecting functions**
           ```
           {question}
           ```
           
            Please exhaustively select as many functions (up to {max_functions}) matching the search criteria.
            Use the provided response schema below to respond.
        
            **Function Data**
            ```json
            {json.dumps(self._functions.to_dict('records'),default=str)}
            ```
        
          ** Response Format**
        
            class FunctionInfo(BaseModel):
                name: str
                reason_for_choosing: str
                rating_percent: float
                
            class UsefulFunctions(BaseModel):
                functions: typing.List[FunctionInfo]
        """

        res.utils.logger.debug(f"selecting up to {max_functions} functions...")
        r = ask(P, as_json=True)
        r = json.loads(r)
        res.utils.logger.debug(r)
        if "functions" in r:
            return r["functions"]
        return list(r.values())[0]["functions"]
