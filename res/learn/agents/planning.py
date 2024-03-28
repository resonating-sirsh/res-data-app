import typing
from typing import Any
from pydantic import BaseModel
import res
import openai
from res.utils import logger
import json
from res.observability.io.index import get_function_index_store
from res.observability.dataops import FunctionDescription
from tenacity import retry, wait_fixed, stop_after_attempt
from res.observability.io import EntityDataStore
from res.utils import get_res_root

DEFAULT_MODEL = "gpt-4-1106-preview"
GPT3 = "gpt-3.5-turbo"
SEED = 42


"""
   HANDLING OF FUNCTIONS AND THE SEARCH OF FUNCTIONS
"""


def read_strategy(name):
    file = get_res_root() / "res" / "observability" / "strategies" / f"{name}.md"
    res.utils.logger.info(f"reading {file}")
    with open(file, "r") as f:
        return f.read()


class FunctionStore:
    """
    wrapper around function search and loading
    """

    def __init__(self, reload=False, allow_entity_apis=False):
        self._function_map = res.connectors.load("redis")["FUNCTIONS"]["MAP"]
        self._function_dict_map = res.connectors.load("redis")["FUNCTIONS"][
            "FUNCTION_DICT_MAP"
        ]

        self._allow_entity_apis = allow_entity_apis
        self._entity_store = EntityDataStore()

        if reload:
            self._load_functions()

    def _load_functions(self):
        res.utils.logger.info(f"populating store")
        store_data = get_function_index_store().load()
        if not self._allow_entity_apis:
            store_data = store_data[
                store_data["function_class"].isin(
                    ["ColumnarQuantitativeSearch", "VectorSearch"]
                )
            ]
        self._function_map["LOOKUP"] = dict(store_data[["name", "text"]].values)
        fs = dict(store_data[["name", "function_description"]].values)

        for k, v in fs.items():
            # logger.debug(f"{k=}, {v=}")
            self._function_dict_map[k] = v

        self._store_data = store_data

    def __getitem__(self, key):
        f = self._function_dict_map[key]

        if f:
            return FunctionDescription.restore(f, alias=key)

        # convenient but we should index it for completeness
        if f == "run_search_for_AbstractEntity":
            return self._entity_store.as_function_description()

        res.utils.logger.debug(f"key not found: {key}")
        return None

    @property
    def functions(self):
        return self._function_map["LOOKUP"]

    def run_function(
        self, function: typing.Callable | str | FunctionDescription, **kwargs
    ):
        """"""
        if isinstance(function, str):
            function = self[function].function
        elif isinstance(function, FunctionDescription):
            function = function.function
        return function(**kwargs)

    def describe_functions_from_stage(self, stage):
        # FunctionCall (s)
        fnames = [f.function_name for f in stage.function_calls]
        # assert we have them all
        return [self[name].function_dict() for name in fnames]


"""
   
    EXECUTION AND PLANNING

"""


class FunctionCall(BaseModel):
    function_name: str
    confidence: float
    # as many as possible is good - cheap to run on store but round trip is bad
    long_form_questions_to_ask: typing.List[str]
    is_function_available: bool
    evidence_that_function_contains_supporting_data_based_on_function_description: str
    # function-type and what we know about the strategy
    # function args to use when calling
    # identified entities from the glossary ??


class Stage(BaseModel):
    stage_index: int
    function_calls: typing.List[FunctionCall]


class StageResponse(BaseModel):
    # we need all the context here so it pulls everything along
    questions_to_ask: typing.List[str]
    full_answer: str
    explanation: str
    confidence_percent: float

    def explain_answer(
        self,
        next_steps="If the question is answered, please terminate now or run another function that you know you can run",
    ):
        return {
            "about": f"We were just asked the question `{self.questions_to_ask}` and then with confidence {self.confidence_percent} we found the answer: {self.explanation}",
            "answer": self.full_answer,
            "next-steps": next_steps,
        }


class Plan(BaseModel):
    question: str
    stages: typing.List[Stage]
    plan_confidence: int
    context: typing.Optional[str]

    def get_stage_with_previous_output(
        self, idx: int, result: typing.Optional[StageResponse] = None
    ):
        return RunnableStage(plan=self, stage_index=idx, previous_stage_result=result)


class PlanResponse(BaseModel):
    plan: Plan
    stages: typing.List[StageResponse]


class RunnableStage(BaseModel):
    plan: Plan
    stage_index: int
    previous_stage_result: typing.Optional[StageResponse]

    @property
    def current_stage(self):
        return self.plan.stages[self.stage_index]

    def get_function_call_reasons(self, function_name):
        for f in self.current_stage.function_calls:
            if f.function_name == function_name:
                return f.long_form_questions_to_ask

    def as_instruction(cls):
        # optionally include the output from the previous stage
        if_not_first_stage = f"""
        The previous stage returned a response:
        **Output from last stage**
        ```json
        {cls.previous_stage_result}
        ```"""

        # generally for the first or later stages format the stuff
        return f"""
        We are trying to carry out the user's question/task below:

        **User Question/Task**
        ```
        {cls.plan.question}
        ```
        
        {if_not_first_stage if cls.stage_index > 0  else ''}

        Now I would like you to execute the following plan:
        **Plan**
        ```json
        {cls.plan.stages[cls.stage_index]}
        ```

        You should return results that satisfy the following Pydantic schema and populate each of the fields
        **Response Format**
       {StageResponse.schema()}
       
        for example if you could send data like
                    
        ```json
        {{
            "questions": ["first question", "second question"] ,
            "full_answer": ...,
            "explanation" : ...,
            "confidence": ...
        }}
                    
        """


class Planner:
    def __init__(cls):
        cls._function_store: FunctionStore = FunctionStore()
        cls._init_messages = [
            {
                "role": "system",
                "content": f"""I'm going to give you a user question. I will also give you a list of functions. I want you to return a detailed plan using the pydantic schema below.
                    Any functions that can be run in parallel in plan should be in the same stage and you can run as many functions as you like.
                    You should consider a number of long form questions to ask functions at each stage to retrieve all the information that is being requested.
                    When running functions in the same stage, do not use any information you would not have yet acquired at this stage and do not refer to any entities or terms in your questions that a given function you are calling would not specifically know about.
                    If you are not confident in calling a function e.g. less than 0.75, suggest a hypothetical but unavailable function to call instead
                    Read the about Resonance details you are given below to make informed decisions about our processes.
                    **Functions**
                    ```json
                        {json.dumps(cls._function_store.functions)}              
                    ```
                    **Response Schema:
                    ```json
                    {Plan.schema_json()}
                    ```""",
            }
        ]

    def __call__(cls, question_or_task, **kwargs):
        return cls.ask_question(question_or_task)

    def ask_question(cls, question, response_format={"type": "json_object"}):
        cls._messages = (
            cls._init_messages
            + [{"role": "system", "content": read_strategy("AboutResonance")}]
            # + [{"role": "system", "content": read_strategy("Search")}]
            + [{"role": "user", "content": question}]
            # + [
            #     {
            #         "role": "user",
            #         "content": "Describe the sequence of events that are described. Make sure yuu understand what Entities are being referenced and causal links between entities and events. You should state that in the final plan `context`",
            #     }
            # ]
            + [
                {
                    "role": "user",
                    "content": "Be comprehensive and determine ALL the entities that you are asked to identify or details you are asked to identify asking as many questions about the entities in the user's question at each stage as you can",
                }
            ]
        )

        res.utils.logger.info("Building plan...")
        response = openai.chat.completions.create(
            model=DEFAULT_MODEL,
            messages=cls._messages,
            # the planner may have access to strategy functions
            # functions=functions_desc,
            response_format=response_format,
            # function_call="auto",
        )

        return json.loads(response.choices[0].message.content)


class Worker:
    """
    A work is a modular agent that
    """

    PLAN = """
    You will be a given a plan to execute. 
    It will provide functions to you that you can call and context in an overall plan.
    Carry out the plan by executing the functions and interpreting the results. Run all functions you are asked to run in the stage before completing.
    Provide comprehensive details by relaying ALL identifiers and facts you are given to the user.
    You can use the entity data store function to lookup details about any entities you observe
    """

    def __init__(cls, context_dict=None):
        # todo loader logic for realz
        cls._function_store = FunctionStore()
        cls._context_dict = context_dict
        cls._entity_store = EntityDataStore()
        cls._entity_function = cls._entity_store.as_function_description()

    def run_plan(cls, plan: Plan):
        """
        run a plan provided by a planner
        loop through each stage in the plan passing result from one stage to the next
        """
        result = None
        results = []
        for i in range(len(plan.stages)):
            stage = plan.get_stage_with_previous_output(i, result=result)
            result = cls(stage)
            results.append(result)
        return PlanResponse(plan=plan, stages=results)

    def ask(cls, question: str, model=None, response_format=None):
        """
        this is a direct request rather than the interpreter mode
        response_format={"type": "json_object"}
        """
        plan = f""" Answer the users question as asked  """

        logger.debug("asking...")
        messages = [
            {"role": "system", "content": plan},
            {"role": "user", "content": f"{question}"},
        ]

        response = openai.chat.completions.create(
            model=model or DEFAULT_MODEL,
            messages=messages,
            response_format=response_format,
        )

        # audit response, tokens etc.

        return response.choices[0].message.content

    # we may fail to parse and we wait in this mode - we could try to coerce later
    # @retry(wait=wait_fixed(0.1), stop=stop_after_attempt(3), reraise=True)
    def agent_ask(
        self,
        questions: str | typing.List[str],
        fnc: typing.Callable | str | FunctionDescription,
        response_model=StageResponse,
        return_raw=False,
        **kwargs,
    ) -> StageResponse:
        """
        ask a question of a function with an agent wrapper to format as per a model
        the input data are labelled with the question below because otherwise the agent will not connect the data with the context necessarily
        """

        logger.debug(f"[agent_ask] {questions=} - {fnc=}, {kwargs=}")

        """
        usually this call some underlying dumb function to get some data
        below we can then format that
        in future an option to just return this to the agent and let it compile is fine too        
        """
        data: str = self.invoke_as_json(fnc, **kwargs)
        logger.debug(f"Function responded raw: {data}")

        if return_raw:
            return data

        """
        Here we ask an interpreter to refine the data for longer planning tasks
        this is done in the full context of the plan/questions/tasks
        """

        @retry(wait=wait_fixed(0.1), stop=stop_after_attempt(3), reraise=True)
        def format(d):
            response = json.loads(
                self.ask(
                    f"""
                    Given the `Input Data` below, answer the `Questions` provided in detail and format your response strictly using the Pydantic `Response Schema` below.
                    Specify each of your (very detailed) answer, explanation, confidence and the question(s) context.
                    If asked to search or example, provide comprehensive details as the user will want specifics relating to identities and events etc and do not omit any identifiers you discovered.
                    The overall goal is provided so that you can include anything that might be useful for the overall goal when compiling answers.
                    
                    **Questions**
                    Overall goal: {self._goal}
                    Current question: {questions}
            
                    **Input Data**
                    These are the {questions}:
                    ```json
                    {d}
                    ``` 
                    **Response Schema (Pydantic)**
                    ```json
                    {StageResponse.schema()}
                    ```
                    
                    for example if you could send data like
                    
                    ```json
                    {{
                      "questions_to_ask": ["first question", "second question"] ,
                      "full_answer": ...,
                      "explanation" : ...,
                      "confidence": ...
                    }}
                    ```
                    """,
                    response_format={"type": "json_object"},
                )
            )
            logger.debug(f"{response=}")
            # if it fails we can retry but this is hella slow and expensive
            return StageResponse(**response)

        return format(data)

    def invoke_as_json(
        cls, fn: typing.Callable | str | FunctionDescription, **kwargs
    ) -> str:
        fn = cls._function_store[fn].function if not callable(fn) else fn

        data = fn(**kwargs)

        return json.dumps(data, default=str)

    def _init_messages(cls, runnable_stage: RunnableStage):
        cls._messages = [
            {"role": "system", "content": cls.PLAN},
            {"role": "user", "content": runnable_stage.as_instruction()},
            {
                "role": "user",
                # could add other contextual dict stuff e.g. like company name, purpose etc - > general context dict
                "content": f"Please note the current date is {res.utils.dates.utc_now()} so you should take that into account if asked questions about time. Also note {cls._context_dict}",
            },
        ]

    def __call__(self, runnable_stage: RunnableStage, **kwds: Any) -> Any:
        return self.run_plan_stage(runnable_stage=runnable_stage)

    def run_plan_stage(
        cls,
        runnable_stage: RunnableStage,
        limit=3,
        response_format={"type": "json_object"},
    ):
        """
        The purpose of the worker is to perform partial work of a greater plan
        the plan's overall objective should be known but the partial context should be enough to answer the question
        """

        cls._goal = runnable_stage.plan.question

        cls._init_messages(runnable_stage)
        cls._loaded_function_descriptions = (
            cls._function_store.describe_functions_from_stage(
                runnable_stage.current_stage
            )
        ) + [cls._entity_function.function_dict()]

        for idx in range(limit):
            # TODO would be nice to pass a stage via the tools model and run all in parallel
            response = openai.chat.completions.create(
                model=DEFAULT_MODEL,
                temperature=0,
                seed=SEED,
                messages=cls._messages,
                # helper that inspects functions and makes the open ai spec
                functions=cls._loaded_function_descriptions,
                response_format=response_format,
                function_call="auto",
            )

            response_message = response.choices[0].message
            logger.debug(response_message)
            function_call = response_message.function_call

            if function_call:
                # we know from the stage why we are calling this function
                # the agent could update this context if we though it was interesting
                question = runnable_stage.get_function_call_reasons(function_call.name)

                function_response: StageResponse = cls.agent_ask(
                    fnc=function_call.name,
                    questions=question,
                    **json.loads(function_call.arguments),
                )
                logger.debug(f"Response: {function_response}")
                content = json.dumps(function_response.explain_answer())

                logger.debug(f"-------------------------------")
                logger.debug(f"Content: {content}")
                logger.info(
                    f"\n<<<<<<<<<<<<<<<<<     {function_response.full_answer} ({function_response.explanation})     >>>>>>>>>>>>>>>>>>\n"
                )
                logger.debug(f"-------------------------------")

                cls._messages.append(
                    {
                        "role": "user",
                        "name": f"{str(function_call.name)}",
                        # explain answer cold be think - i.e. literally the response but we could reframe too
                        "content": content,
                    }
                )

            if response.choices[0].finish_reason == "stop":
                break

        try:
            j = json.loads(response_message.content)
            try:
                return StageResponse(**j)
            except:
                return j
        except:
            return response_message.content or "There were no data found"
