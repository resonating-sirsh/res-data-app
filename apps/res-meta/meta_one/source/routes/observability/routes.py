from res.utils import logger
from fastapi import APIRouter
from fastapi_utils.cbv import cbv
import typing
from pydantic import BaseModel
from res.observability.entity import AbstractVectorStoreEntry
from res.observability.io import VectorDataStore, ColumnarDataStore
from res.utils.secrets import secrets
import os
import res
from fastapi import APIRouter, Depends
from fastapi import Depends, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from res.learn.agents.builder import (
    FunctionManager,
    ResAgent,
    FunctionDescription,
    AgentConfig,
)


def verify_api_key(key: str):
    RES_META_ONE_API_KEY = secrets.get_secret("RES_META_ONE_API_KEY")

    if key == RES_META_ONE_API_KEY:
        return True
    else:
        raise HTTPException(
            status_code=401,
            detail="Unauthorized. Invalid RES_META_ONE_API_KEY.  Please refresh this value from AWS secrets manager",
        )


security = HTTPBearer()


def get_current_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    if not verify_api_key(token):
        raise HTTPException(
            status_code=401,
            detail="Invalid  RES_META_ONE_API_KEY in token check. Check AWS Secrets for this value",
        )
    return token


class FunctionSearch(BaseModel):
    namespace: str = None
    types: typing.List[str] = None


class CreateAgentPayload(BaseModel):
    name: str = None
    namespace: str = "default"
    prompt: str
    function_search_prompt: str
    max_functions: int = 7


class VectorSearch(BaseModel):
    questions: typing.Union[str, typing.List[str]]
    limit: int = 7
    function_name: str = None
    function_namespace: str = None
    use_agent: bool = False
    # other options


def get_observability_routes() -> APIRouter:
    router = APIRouter()

    try:
        os.environ["OPENAI_API_KEY"] = secrets.get_secret("ASK_ONE_OPENAI_API_KEY")
    except:
        pass

    @cbv(router)
    class _Router:
        @router.get("/observability/agent-search-keystore", name="agent search")
        def agent_search_keystore(
            self,
            name: str,
            question: str,
            namespace: str = "default",
            token: str = Security(get_current_token),
        ) -> dict:
            """
            experimental endpoint to make requests against a known agent
            we should probably move the agent calls to ane api eventually

            some examples in the default namespace are `queue_agent` or `resonance_context_agent`

            **Args**
              name: the name of the agent eg. enum queue_agent|resonance_context_agent
              namespace: the namespace for the agent enum default|None
              question: ask a question of the agent
            """
            fm = FunctionManager()
            key = f"run_search_for_{namespace or 'default'}_{name}"
            if not key:
                raise HTTPException(f"Could not locate agent {key} in store")
            f = fm[key].function

            return {"response": f(question)}

        @router.post(
            "/observability/create-agent",
            name="create or update agent by name",
            response_model=AgentConfig,
        )
        def create_agent(
            self,
            config: CreateAgentPayload,
            token: str = Security(get_current_token),
        ) -> AgentConfig:
            fm = FunctionManager()
            config = fm.function_search_agent_config(
                config.name,
                namespace=config.namespace,
                max_functions=config.max_functions,
                prompt=config.prompt,
                function_search_prompt=config.function_search_prompt,
            )
            ResAgent(config).dump(register=True)
            return config

        @router.post(
            "/observability/add-store-content",
            name="add-store-content",
        )
        def add_content_to_store(
            self,
            store_namespace: str,
            store_name: str,
            text: str,
            key: typing.Optional[str] = None,
            doc_id: typing.Optional[str] = None,
            token: str = Security(get_current_token),
        ) -> dict:
            """
            add content to a vector store
            """

            key = key or res.utils.res_hash()
            logger.info(f"Adding text to store")
            Model = AbstractVectorStoreEntry.create_model(
                name=store_name, namespace=store_namespace
            )
            data = Model(
                name=key,
                text=text,
                doc_id=doc_id or key,
                timestamp=res.utils.dates.utc_now_iso_string(),
            )
            VectorDataStore(Model).add(data)

            return {"message": "rows_added", "key": key}

        @router.post(
            "/observability/add-l2-store-content",
            name="add-l2-store-content",
        )
        def add_content_to_l2_store(
            self,
            text: str,
            key: typing.Optional[str] = None,
            doc_id: typing.Optional[str] = None,
            token: str = Security(get_current_token),
        ) -> dict:
            """
            add content to a vector store
            """

            key = key or res.utils.res_hash()
            logger.info(f"Adding text to store")
            Model = AbstractVectorStoreEntry.create_model(
                name="l2_docs", namespace="executive"
            )
            data = Model(
                name=key,
                text=text,
                doc_id=doc_id or key,
                timestamp=res.utils.dates.utc_now_iso_string(),
            )
            VectorDataStore(Model).add(data)

            return {"message": "rows_added", "key": key}

        @router.post(
            "/observability/list-functions",
            name="list-functions",
        )
        def list_functions(self, options: FunctionSearch) -> typing.List[dict]:
            """
            list all functions that the RAG system exposes
            you can filter by types

            """
            from res.learn.agents import InterpreterAgent

            logger.info(f"Loading the agent and fetching functions")

            agent = InterpreterAgent()
            data = agent._function_index_store.load()[
                ["name", "text", "function_namespace", "function_class", "id"]
            ]

            if options.namespace:
                data = data[data["function_namespace"] == options.namespace]

            if options.types:
                data = data[data["function_class"].isin(options.types)]

            return data.to_dict("records")

        @router.get(
            "/observability/search-keystore",
            name="search-keystore",
        )
        def search_keystore(
            self,
            key_or_keys: str = None,
            token: str = Security(get_current_token),
        ) -> typing.List[dict]:
            """
            use the key lookup
            """
            from res.observability.io.EntityDataStore import Caches

            from res.observability.io import (
                EntityDataStore,
            )
            from res.observability.entity import (
                AbstractEntity,
            )

            store = EntityDataStore(AbstractEntity, Caches.QUEUES)

            result = store(key_or_keys)

            if not isinstance(result, list):
                result = [result]
            return result

        @router.post(
            "/observability/inspect_contract_variables",
            name="inspect_contract_variables",
        )
        def inspect_contract_variables_store(
            self, search_options: VectorSearch
        ) -> typing.List[dict]:
            """
            cerates an agent from the store for contract variables and search based on the question with formatted results
            """
            model = AbstractVectorStoreEntry.create_model(
                "contract_variables_text",
                namespace="contracts",
            )

            try:
                store = VectorDataStore(
                    model,
                    create_if_not_found=True,
                    description="Glossary of contract variables with text for Resonance with owners, nodes, commercial acceptability terms",
                )
                if search_options.use_agent:
                    store = store.as_agent()
                    # the returns an answer
                    return [
                        {
                            "response": store(
                                search_options.questions, limit=search_options.limit
                            )
                        }
                    ]
                else:
                    # this returns a list
                    return store(search_options.questions, limit=search_options.limit)

            except Exception as ex:
                return {"message": "failed to retrieve data"}

        @router.post(
            "/observability/search_store",
            name="search_store",
        )
        def search_store(
            self,
            search_options: VectorSearch,
            token: str = Security(get_current_token),
        ) -> typing.List[dict]:
            """
            search the stores in parallel - you can use this to search for queue data. for example in the search options specify namespace as `queue` and name as one of `Body`, `Style`, `Order`, `ProductionRequest`
            and then ask one or more questions of the store
            """
            model = AbstractVectorStoreEntry.create_model(
                name=search_options.function_name,
                namespace=search_options.function_namespace,
            )

            results = []
            try:
                store = ColumnarDataStore(model, create_if_not_found=False)
                results += store.run_search(
                    queries=search_options.questions, limit=search_options.limit
                )
            except Exception as ex:
                res.utils.logger.warn(ex)

            try:
                store = VectorDataStore(model, create_if_not_found=False)
                results += store.run_search(
                    queries=search_options.questions, limit=search_options.limit
                )
            except Exception as ex:
                res.utils.logger.warn(ex)

            return results

        @router.post(
            "/observability/search_l2_doc_store",
            name="search_l2_doc_store",
        )
        def search_l2_doc_store(
            self,
            search_options: VectorSearch,
            token: str = Security(get_current_token),
        ) -> typing.List[dict]:
            """
            search the stores in parallel
            """
            model = AbstractVectorStoreEntry.create_model(
                name="l2_docs", namespace="executive"
            )
            store = VectorDataStore(model, create_if_not_found=True)
            # TODO filter by dates and other content types
            return store.run_search(
                queries=search_options.questions, limit=search_options.limit
            )

        @router.get(
            "/observability/search_quality_store",
            name="search quality store",
        )
        def search_quality_store(
            self,
            question: str,
            limit: int = 10,
            token: str = Security(get_current_token),
        ) -> typing.List[dict]:
            """
            search the quality notes stores

            **Args**
                question: ask your question(s)
                limit: limit vector store search results
            """
            res.utils.logger.info(f"{question=}")
            model = AbstractVectorStoreEntry.create_model(
                namespace="quality", name="quality_assessment"
            )
            store = VectorDataStore(model, create_if_not_found=True)
            # TODO filter by dates and other content types
            result = store.run_search(queries=question, limit=limit)
            res.utils.logger.info(result)
            return result

        @router.get(
            "/observability/get_l2_doc_store_search",
            name="get_l2_doc_store_search",
        )
        def search_l2_doc_store_get(
            self,
            question: str,
            limit: int = 10,
            token: str = Security(get_current_token),
        ) -> typing.List[dict]:
            """
            search the stores in parallel
            """
            model = AbstractVectorStoreEntry.create_model(
                name="l2_docs", namespace="executive"
            )
            store = VectorDataStore(model, create_if_not_found=True)
            # TODO filter by dates and other content types
            return store.run_search(queries=question, limit=limit)

        @router.post(
            "/observability/plan",
            name="plan",
        )
        def plan(
            self,
            question: str,
            token: str = Security(get_current_token),
        ) -> typing.List[dict]:
            """
            given a question provide the plan over functions
            """
            return [{"todo": "cool"}]

        @router.get(
            "/observability/uriSigner",
            name="S3 Uri Signer",
        )
        def uri_signer(
            self,
            uris: str,
            token: str = Security(get_current_token),
        ) -> typing.List[str]:
            """
            sign s3 file uris

            **Args**
              uris: provide one or more s3://uris as a comma separated list
            """
            s3 = res.connectors.load("s3")

            uris = uris.split(",")
            return [s3.generate_presigned_url(u) for u in uris]

        # todo - index a function
        # add summarization task
        # entity store lookup
        #

    return router


router = get_observability_routes()
