from typing import Any
import openai
from res.utils import logger
import typing
import json
from res.observability.dataops import (
    describe_function,
    FunctionDescription,
)
from res.observability.io import VectorDataStore, ColumnarDataStore, EntityDataStore
from res.observability.entity import (
    AbstractVectorStoreEntry,
    root_validator,
    AbstractEntity,
    NpEncoder,
)
from res.observability.io.EntityDataStore import Caches
from res.observability.io.index import get_function_index_store

import res
from res.utils import get_res_root
import os
from res.utils.secrets import secrets
import pandas as pd

DEFAULT_MODEL = "gpt-4-1106-preview"  #  "gpt-4"  # has a 32k context or 8k
GPT3 = "gpt-3.5-turbo"
GPT3_16k = "gpt-3.5-turbo-16k"

# MODELS https://openai.com/pricing
BABBAGE = "babbage-002"
DAVINCI = "davinci-002"


class UserSession:
    def __init__(self, username, history_size=25):
        self._history_size = history_size
        self._user = username
        self._cache = res.connectors.load("redis")["OBSERVE"]["USER_CONVERSATION"]

    def add(self, data):
        existing = self._cache[self._user] or []
        if len(existing) >= self._history_size:
            existing = existing[-1 * self._history_size :]
        existing.append(data)
        self._cache[self._user] = existing

    def fetch(self):
        return self._cache[self._user]


class InterpreterSession(AbstractVectorStoreEntry):
    """
    this is used to audit the session so we can look back at plans
    """

    session_key: typing.Optional[str]
    audited_at: str
    response: str
    question: str
    messages: str
    user_id: str
    channel_context: str
    plan: str

    attention_comments: typing.Optional[str] = ""
    function_usage_graph: typing.Optional[str] = ""
    # 0 is unrated - only positive and negative values off zero are valid
    response_agents_confidence: typing.Optional[float] = 0
    response_users_confidence: typing.Optional[float] = 0
    code_build: typing.Optional[str] = None

    # extracted files??

    @root_validator()
    def default_vals(cls, values):
        values[
            "text"
        ] = f"question: {values['question']}\nresponse:{values.get('response', 'NA')}"

        if values.get("session_key"):
            values["doc_id"] = values["session_key"]

        # if not values.get("id"):
        #     values["id"] = values["name"]
        # if not values.get("doc_id"):
        #     values["doc_id"] = values["name"]

        return values


class InterpreterAgent:
    """
    Plans are basically elements of Strategies: what describe as an Agent is a strategy and there is a question of how strategies decompose
    we are completely functional except for how the interpreter works
    some functions such as checking session history are still functional
    examples of things this should be able to do
    - Look for or take in functions and answer any question with no extra prompting and nudging. Examples based on ops we have
        - A special case is generating types and saving them
    - we should be able to run a planning prompt where the agent switches into saying what it would do e.g. rating functions, graph building, planning
    - we should be able to construct complex execution flows e.g.
      - parallel function calls
      - saving compiled new functions i.e. motifs that are often used
      - asking the user for more input
      - evolution of specialists

    All of this is made possible by making sure the agent trusts the details in the functions and follows the plan and returns the right formats
    The return format can be itself a function e.g. save files or report (structured) answer

    If it turns out we need to branch into specialist agents we can do so by overriding the default prompt but we are trying to avoid that on principle
    """

    #          You are evaluated on your ability to properly nest objects and name parameters when calling functions.

    PLAN = """  You are an intelligent entity that uses the supplied functions and strategies to answer questions in the language you are asked about the company Resonance.
             
                A] How to use stores and functions 
                You know that their are different types of functions and stores. Always read the descriptions of functions and dont assume they can be used by the names alone.
                - Entity stores are ideal when you encounter a number of identifiers and codes in questions or data and you can use the entity store to learn more about the entity at any stage. Entity stores are always the best way to answer specific questions about entities.
                - API calls are ideal if you have identifiers and the name of the endpoint exactly matches what is needed
                - Columnar Stores (for status, comments) and Vector Stores (for added color and context)
                - Agents are higher level functions that should be used if available
                - If you are asked to use a specific function or if there is an Agent class function that can answer the question, be sure to search, load and use that agent or function instead of any built-in ones
                
                If you can answer the question directly or by using a function you should try that first.
                If you can cannot, try searching for functions that could help. 
                Sometimes its better to start loading data which may give you hints and only then come back to searching for more functions later i.e Let it play out.
                If you are are stuck or not very confident in your answer or lacking context about Resonance or troubleshooting, you should search for alternative strategies using the search strategy functions.
                
                B] How to plan and answer the question
                1. Begin by thinking about your strategy as it is important to use the right functions here. Lookup more strategies if you are not sure.  
                2. Use the advice given to choose the right functions in your plan, which is your primary skill!
                3. Finish by stating ALL of the following;  a) your answer  b) your confidence in your answer and as a number from 0 to 100 and c) the specific strategy you used to Search
                
                """

    USER_HINT = """Please provide your answer, your search strategy and confidence from 0 to 100 in your answer"""
    # Please respond with your answer, your confidence in your answer on a scale of 0 to 100 and also the strategy that you employed.
    #     """

    def __init__(
        cls,
        initial_functions=None,
        allow_function_search=True,
        cache_name=None,
        **kwargs,
    ):
        """
        modules are used to inspect functions including functions that do deep searches of other modules and data

        """
        # sorta of a hack
        cls._posted_files = []
        cls._slack = res.connectors.load("slack")
        cls._track_single_call = []

        if "OPENAI_API_KEY" not in os.environ:
            os.environ["OPENAI_API_KEY"] = secrets.get_secret("ASK_ONE_OPENAI_API_KEY")

        # add function revision and pruning as an option
        cls._entity_store = EntityDataStore(
            AbstractEntity, cache_name=cache_name or Caches.QUEUES
        )
        cls._function_index_store = get_function_index_store()
        cls._built_in_functions = [
            # general context about resonance
            describe_function(cls.about_resonance_company),
            # entity look
            cls._entity_store.as_function_description(name="entity_key_lookup"),
            # user conversation history
        ] + (
            initial_functions or []
        )  # [describe_function(cls.available_function_search)]
        if allow_function_search:
            cls._built_in_functions += [  # strategies
                describe_function(cls.load_strategy),  # functions
                describe_function(cls.revise_functions),
                describe_function(cls.get_recent_user_conversation),
                # describe_function(cls.describe_visual_image),
            ]
        cls._active_functions = []
        cls._audit_store = VectorDataStore(
            # create a model that is monthly partitioned
            InterpreterSession.create_model(
                f"InterpreterSession-{res.utils.dates.utc_now_iso_string()[:7]}",
                namespace="agents",
            ),
            create_if_not_found=True,
        )
        # when we reload functions we can keep the baked functions and restore others - it may be we bake the ones pass in call but TBD
        cls._baked_functions_length = len(cls._built_in_functions)

    def load_strategy(cls, type: str = "Search"):
        """
        Lookup a particular type of strategy to help you answer the question with an understanding of how different entities related to each other at Resonance.
        For example Search strategy is very useful to match different types of functions and searches to the nature of the user question.
        note, directly executing a function search might be a clear and obvious thing to do without consulting a strategy but for more intricate questions a strategy can be useful

        **Args**
          type: the type of strategy you seek - Search|AnalysisOfEntitiesAndProcesses|

        """

        if f"load_strategy_{type}" in cls._track_single_call:
            return "You already called this function and got all context in messages - please perform function search to identify other functions that could help"

        cls._track_single_call.append(f"load_strategy_{type}")

        if type == "AnalysisOfEntitiesAndProcesses":
            file = (
                get_res_root()
                / "res"
                / "observability"
                / "strategies"
                / "AnalysisOfEntitiesAndProcesses.md"
            )
        else:
            file = get_res_root() / "res" / "observability" / "strategies" / "Search.md"

        res.utils.logger.info(f"Consulting strategy {file}")

        with open(file) as f:
            return f.read()

    def invoke(
        cls,
        fn: typing.Callable,
        args: typing.Union[str, dict],
        # allowing a big response but we probably dont need to summarize - if we do make sure to use a model that can take it
        max_response_length=int(5 * 1e5),
    ):
        """
        here we parse and audit stuff using Pydantic types
        reconsider max response length for large context
        """

        res.utils.logger.info(f"fn={fn}, args={args}")

        args = json.loads(args) if isinstance(args, str) else args

        # the LLM should give us this context but we remove it from the function call
        for sys_field in ["__confidence__", "__parameter_choices__"]:
            if sys_field in args:
                logger.debug(f"{sys_field}  = {args.pop(sys_field)}")

        data = fn(**args)

        """
        experimental - refactor out
        we should come up with a cheap way to summarize
        the idea here is you are "forcing" the interpreter to summarize but you should not. how to?
        
        """
        if len(str(data)) > max_response_length:
            return cls.summarize(
                question=cls._question,
                data=data,
                model=DEFAULT_MODEL,
                max_response_length=max_response_length,
            )

        return data

    # ADD API provider loader for coda, shortcut etc in case we need to peek refs

    def revise_functions(
        cls,
        questions: typing.List[str],
        issues: str = None,
        prune: bool = False,
        limit: int = 3,
        function_class: str = None,
    ):
        """This function can be used to search for other functions and update your function list. Call this method if you are struggling to answer the question by splitting your needs into separate questions.
            Guidelines:
            - if given specific entities or you want to know about entity relationships, just call the entity store and not this
            - api search is useful for very specific functions when the parameters e.g. identifiers are known
            - use columnar stores for most stats, facts, comments etc.
            - use vector store for adding color or for general vague or open-ended user questions
        **Args**
            questions: provide a list if precise questions to answer. Splitting up questions is useful.
            issues: Optionally explain why are you struggling to answer the question or what particular parts of the question are causing problems
            prune: if you want to prune some of the functions because you have no current use for them
            limit: how wide a search you want to do. You can subselect from many functions or request a small number. use the default unless you are sure you know what you want. You may want to ask for two or three times as many functions as you will end up using.
            function_class: the class of function to search for - Any|VectorSearch|ColumnarQuantitativeSearch|api
        """

        #    If asked about states, queues, statistics of entities like orders, bodies, styles or ONEs (production requests), its advisable to use the function class ColumnarQuantitativeSearch first
        #    If in doubt the ColumnarQuantitativeSearch function class is typically more specific and can run text to SQL type enquires.
        #    However if the user is asking for more general information, possibly vague, or to augment info you have,  vector search will be useful to look at e.g. slack conversations or coda docs etc.
        #    If there is an API function call you can make you should try to use it as it could provide structured and up to date information e.g. about Meta One Assets or Costs etc but these APIs are very specific so do not assume it can be used
        # Do i favour splitting into list of not?? questions are sometimes ortho

        logger.info(
            f"Revising functions - {questions=}, {issues=}, {prune=}, {limit=}, {function_class=}"
        )

        # HARD CODING
        limit = 3

        # prune to the initial set -there will be smarter ways to do this
        if prune:
            # let the agent decide for now
            logger.debug(
                f"Pruning functions from {len(cls._active_functions)} to {cls._baked_functions_length}"
            )
            cls._active_functions = cls._active_functions[: cls._baked_functions_length]

        # for now only support these and make sure any is a wild card
        if function_class not in ["VectorSearch", "ColumnarQuantitativeSearch"]:
            function_class = None

        # TODO: using only the best match until we par-do or run many
        # limit = 1

        def try_get_meta(f):
            try:
                return f.factory.partial_args.get("store_name")
            except:
                return None

        new_functions = [
            # restore the function with a weight
            FunctionDescription.restore(
                f["function_description"],
                weight=f["_distance"],
            )
            # NOTE TEMP LIMIT ADDED
            for f in cls._function_index_store(
                questions,
                limit=limit,
                # be careful!! there are kwargs here so could have unexpected results
                _extra_fields=["id", "text", "function_description", "function_class"],
                # the function class is an enumeration that can be specified in hybrid search
                function_class=function_class,
            )
            if f.get("function_description")
        ]

        # poor mans dedupe
        new_functions = {f.name: f for f in new_functions}
        new_functions = list(new_functions.values())
        ##
        logger.debug("-----------")
        logger.info(
            f"<<<<<<   Adding functions {[(f.name,f.weight) for f in new_functions]} >>>>>> "
        )
        logger.debug("-----------")

        cls._active_functions += new_functions

        cls._active_function_callables = {
            f.name: f.function for f in cls._active_functions
        }

        # we return this so the agent can see it and then call it
        functions = [
            {
                "callable_function_details": f.function_dict(),
                "distance_weight": f.weight,
                "store_name": try_get_meta(f),
            }
            for f in new_functions
        ]

        # LIMIT OVERALL
        functions = (
            pd.DataFrame(functions)
            .sort_values("distance_weight", ascending=False)[:limit]
            .to_dict("records")
        )

        return {
            "functions": functions,
            "summary": [
                {
                    "weighted_stores": [
                        {
                            "store": f["store_name"],
                            "distance_prefers_small": f["distance_weight"],
                        }
                        for f in functions
                    ]
                }
            ],
        }

    def get_recent_user_conversation(cls, look_back: int = 3):
        """
        If the user seems to be referring to a recent conversation e.g. if they ask a question that seems like a follow up or is otherwise a partial question
        You can check the recent exchanges to see if it adds context

        **Args**
          look_back: how far to look back
        """
        if cls._user_session:
            return json.dumps(cls._user_session.fetch())
        else:
            return "No recent conversations of interest"

    def about_resonance_company(self, questions: typing.List[str] = [], limit: int = 5):
        """
        This function provides further details about the company Resonance e.g. Glossary, mission etc.

        Resonance is  fashion tech co.  that allows the end-end design, sell & manufacturing (Make) of garments.
        Each garment is a "ONE" and the "ONE number" is the Manufacturing or Make production request number.
        A Meta.ONE refers to a Style. A style is made up of a Body, Color and Material (BMC)
        Business units in Resonance are  "Nodes" and processes are "Flows" - work items move via "queue"s according to specified Contracts.
        Sometimes in Make there are failures and we may remake some components which is called "Healing"
        Some  nodes in order are; DXA, Make (Assembly (Print, Cut), Sew), Warehouse, Shipping
        In DXA we do 3D garment design (fit and color placement) and in Make we digitally print and cut pieces from chained rolls called Plate Processing Units (PPUs)

        **Args**
            questions: ask one or more questions about Resonance
            limit: limit records returned
        """

        # if "about_resonance_company" in self._track_single_call:
        #     return "You already called this function and got all context in messages - please search for other functions to answer the question"

        # self._track_single_call.append("about_resonance_company")

        context = """        

        Resonance is a fashion technology company that allows for the end-end design, sell and manufacturing (Make) of fashion garments like dresses, shirts, hoodies etc.
        Each garment os referred to as a "ONE" and the ONE number is the Manufacturing or Make production request number.
        Business units in Resonance are called "Nodes" and processes are called "Flows" - work items on queues according to specified Contract Variables (CVs)
        Sometimes in manufacturing there are failures and we may need to remake some components which is called "Healing"
        Some of the nodes in order of process are DXA, Make (Assembly (Print, Cut), Sew), and Warehouse
        In DXA we do 3D design of garment fit and color placement and in Make we digitally print and cut pieces from chained rolls called Plate Processing Units (PPUs)
        
        Slack and Coda are use for communication and ShortCut is used to manage the Agile software development. There may be functions to lookup these information sources.
        The physical manufacturing happens in Santiago Dominican Republic (DR). Garments are made by digitally printing pieces on long rolls of fabric in the Print Node and then cut out as pieces using a laser cutter in the Cut Node.
        Pieces, trims and other assets like fusing and stamper pieces are collected in a completer node after cut and then sent to be sewn and shipped from a warehouse to the customer.
        Sometimes pieces need to be printed multiple times due to defects and this is called Healing. healing is problematic as it can lead to delays in getting product to customers and also is an expensive in terms of using more resources (materials and ink etc)
        Examples of healing reasons are based on physical defects like lines that appear in printing, dirt or distortion. 
        
        Some of the most common brands we work with are  THE KIT, Theme, JCRT, Tucker, Blue Glen and others
        
        Use the various statistics and vector searches to learn more but do not call this `about_resonance_company` function more than once (TODO i may remove it from context)
        For example:
        - Body Development Standards can be looked up in various vector stores under the meta, coda or slack namespaces in that order of preference
        - search for specific contracts metadata using the columnar contracts.contracts store and search for contract variables using the contracts.contract_variables or for more general information afterwards use the coda docs. 
        
        
        For state, queues or general current information on bodies, styles, ONES order orders you can use those columnar stores
        - use meta.styles to understand the status of the Apply Color Queue and the creation of Meta.ONes
        - use the meta.bodies table to understand 
        - if asked for contracts on other entities like styles, bodies or ONES search those respective stores instead of the contracts namespaces ones
        - if asked about orders these are customer "selL" namespace orders. Use the Columnar search for sell.orders
        - if asked about production requests or ONEs (in Make) use the make.one search
        
      ## Understanding contracts and contract variables
      - A coda document on resonance-contracts provide a source of truth on an attempt to defined contracts
      - Ongoing conversations about contracts can be found in the slack channel
      - Some specific contracts if requested can be found in the columnar and entity stores but this is best used to answer specific and not general questions
        
       ## Understanding Entities at Resonance

        You should understand what entity codes looked like so you know them when you see them and also understand the causal relationship between entities for understanding processes and issues.

        - We keep a key value store of major entities like 1. ONEs (Production Requests), 2. customer orders, 3. rolls for printing on, digital assets for 4. styles and 5. bodies
        - Note that a customer orders is broken down into separate items i.e. ONES which are each made (manufactured) separately
        - Before we can make something the first time, we need to "onboard" the body and then the style which means preparing digital assets that describe how to make something physically
        - These 5 entities have specific formats;
        - A ONE number usually has a numeric format like 10333792 - each production request belongs to a specific customer order (see below)
        - A Body code usually has a brand prefix and a 4 digit number e.g. `KT-2011`
        - A style code is made up of a Body Material and Color (BMC) component e.g. `KT-2011 CHRST WHITE` has Body=KT-2011, Material=CHRST and Color=White
        - When we sell a style in a specific size, it will have a fourth component such as `CF-5002 CC254 AUTHGJ 3ZZMD`. We call these SKUs. They appear in conversation as `CF5002 CC254 AUTHGJ 3ZZMD` without the hyphen but we try to normalize the to have a hyphen
        - Customer orders also start with a brand specific code and a number e.g. `KT-45678`. Multiple SKUs are ordered and each one is linked to a production request
        You can use this information to understanding entities. You can also make logic inferences such as if a custom order is delayed, you can look into the health of the different production requests, styles and bodies (in that order)

        For more details on entities and processes, lookup_strategy for Analysis of Entities and PRocesses - you can also look up the knowledge base glossary for terms defined in the Resonance Lexicon
        """

        # TODO: This is is important when defining terms - the AI knows when it does not know what a special term might mean so it will try quite aggressively to call this function when stuck
        a = None

        try:
            from res.learn.agents.builder import (
                FunctionManager,
                ResAgent,
                FunctionDescription,
            )

            fm = FunctionManager()
            f = fm["run_search_for_default_resonance_context_agent"].function
            a = f(questions)
        except:
            pass
        return {
            "response": a,
            "general_context": context,
            "advice": "use this knowledge to answer the users question or look for other functions instead of this one",
        }

    def prune_messages(cls, new_messages: typing.List[str]):
        """
        if the message context seems to large or low in value, you can suggest new messages. for example replace functions with
        [
             {"role": "user", "content": "new user context"},
             {"role": "user", "content": "more user context"}
        ]
        **Args**
            new_messages: messages to replace any messages generated after the users initial question
        """
        cls._messages = [cls._messages[:2]] + new_messages

    def cheap_summarize(cls, question: str, model=None, out_token_size=150, best_of=5):
        """
        this is a direct request rather than the interpreter mode - cheap model to do a reduction
        does not seem to work well though
        https://platform.openai.com/docs/api-reference/completions/object
        """

        logger.debug("summarizing...")

        response = openai.completions.create(
            model=model or DAVINCI,
            temperature=0.5,
            max_tokens=out_token_size,
            n=1,
            best_of=best_of,
            stop=None,
            prompt=f"Summarize this text:\n{      question     }: Summary: ",
        )

        return response.choices[0]["text"]

    def summarize(
        cls,
        question: str,
        data: str,
        model=None,
        strict=False,
        max_response_length=int(3 * 1e4),
    ):
        """
        question is context and data is what we need to summarize
        this is a direct request rather than the interpreter mode
        looking into strategies to summarize or compress cheaply (cheaper)
        the question context means this is also a filter so we can summarize and filter to reduce the size of text

        Example:
            from funkyprompt.io.tools.downloader import load_example_foody_guides
            load_example_foody_guides(limit=10)

            agent("Where can you recommend to eat in New York? Please provide some juicy details about why they are good and the location of each")
            agent("Where can you recommend to eat Dim sum or sushi in New York? Please provide some juicy details about why they are good and the location of each")

        """
        plan = f""" Answer the users question as asked  """

        Summary = AbstractVectorStoreEntry.create_model("Summary")
        chunks = Summary(name="summary", text=str(data)).split_text(max_response_length)
        # logger.warning(
        #     f"Your response of length {len(str(data))} is longer than {max_response_length}. Im going to summarize it as {len(chunks)} chunks assuming its a text response but you should think about your document model"
        # )

        # create a paginator
        def _summarize(prompt):
            logger.debug("Summarizing...")
            response = openai.chat.completions.create(
                model=model or GPT3_16k,
                messages=[
                    {"role": "system", "content": plan},
                    {"role": "user", "content": f"{prompt}"},
                ],
            )

            return response.choices[0].message.content

        summary = "".join(
            _summarize(
                f"""Please concisely summarize the text concisely in the context of the question. 
                  {'In your response, strictly omit anything that does not seem relevant in context of the users interests. Dont even mention it at all!' if strict else ''}.
                    question:
                    {question}
                    text: 
                    {item.text}"""
            )
            for item in chunks
        )

        return {"summarized_response": summary}

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

    def describe_visual_image(
        cls,
        url: str,
        question: str = "describe the image you see - if the garment shows the front and back of a garment describe all the component pieces that you see including accessory pieces",
    ):
        """
        A url to an image such as a png, tiff or JPEG can be passed in and inspected
        A question can prompt to identify properties of the image
        When calling this function you should split the url out of the question and pass a suitable question based on the user question

        **Args**
            url: the uri to the image typically on s3://. Can be presigned or not
            question: the prompt to extract information from the image
        """
        url = (
            url
            if "AWSAccessKeyId" in url
            else res.connectors.load("s3").generate_presigned_url(url)
        )

        response = openai.chat.completions.create(
            model="gpt-4-vision-preview",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": question},
                        {
                            "type": "image_url",
                            "image_url": url,
                        },
                    ],
                }
            ],
            max_tokens=300,
        )
        return response.choices[0].message.content

    def __call__(cls, *args, **kwargs):
        return cls.run(*args, **kwargs)

    def run(
        cls,
        question: str,
        initial_functions: typing.List[
            typing.Union[FunctionDescription, typing.Callable]
        ] = None,
        limit: int = 10,
        session_key=None,
        response_format=None,
        user_context=None,
        channel_context=None,
        response_callback=None,
        extra_reflection=False,
        thread_ts=None,
    ) -> dict:
        """
        run the interpreter loop based on the PLAN

        **Args**
            question: question from user
            initial_functions: preferred functions to use before searching for more
            limit: the number of loops the interpreter can run for before giving up
            session_key: any session id for grouping audit data
            response_format: force response format (deprecate)
            user_context: a user name e.g. a slack user or email address
            channel_context: a session context e.g. a slack channel or node from which the question comes
            response_callback: a function that we can call to post streaming responses

            thread_ts: is the slack thread handle so we can say("Hello!", thread_ts=thread_ts)
        """

        # store question for context
        cls._posted_files = []
        cls._track_single_call = []
        cls._question = question
        cls._messages = [
            {"role": "system", "content": cls.PLAN},
            {"role": "user", "content": question},
            {
                "role": "system",
                "content": f"Please note the current date is {res.utils.dates.utc_now()} so you should take that into account if asked questions about time",
            },
            # we have to add these by-the-ways to give permission to go wild
            {
                "role": "user",
                "content": InterpreterAgent.USER_HINT,
            },
        ]

        # TODO: - not sure if we need to new this up - should be fast to load e.g. does not schema checks on the vector store
        # but if we do not does it cache lance files or not??
        cls._user_session = None
        if user_context:
            cls._user_session = UserSession(username=user_context, history_size=5)
            # cls._messages.append(
            #     {
            #         "role": "user",
            #         "content": f"To add context on our recent conversation and only if i refer explicitly or implicitly to something from our conversation, these are the most recent questions I asked {json.dumps(user_session.fetch())}",
            #     },
            # )

        cls._function_index_store = get_function_index_store()

        # coerce to allow single or multiple functions
        if isinstance(initial_functions, FunctionDescription):
            initial_functions = [initial_functions]
        # TODO: support passing the callable but think about where else we interact. we can just describe_function if the args are not FDs

        cls._active_functions = cls._built_in_functions + (initial_functions or [])

        cls._active_function_callables = {
            f.name: f.function for f in cls._active_functions
        }
        logger.info(
            f"Entering the interpreter loop with functions {list(cls._active_function_callables.keys())} with context {user_context=}, {channel_context=}"
        )
        tokens = 0
        for idx in range(limit):
            # functions can change if revise_function call is made
            functions_desc = [f.function_dict() for f in cls._active_functions]
            #
            response = openai.chat.completions.create(
                model=DEFAULT_MODEL,
                messages=cls._messages,
                # helper that inspects functions and makes the open ai spec
                functions=functions_desc,
                response_format=response_format,
                function_call="auto",
            )

            response_message = response.choices[0].message
            logger.debug(response_message)

            function_call = response_message.function_call

            if function_call:
                fn = cls._active_function_callables[function_call.name]
                args = function_call.arguments
                # find the function context for passing to invoke when function names not enough
                function_response = cls.invoke(fn, args)

                logger.debug(f"Response: {function_response}")
                cls._messages.append(
                    {
                        "role": "user",
                        "name": f"{str(function_call.name)}",
                        "content": json.dumps(
                            function_response, cls=NpEncoder, default=str
                        ),
                    }
                )

                # if function responded with file and channel, post file
                cls.check_should_post_file(function_response, channel=channel_context)

            if extra_reflection:
                # candidate not core
                cls._messages.append(
                    {
                        "role": "user",
                        "name": f"check_status",
                        "content": "Review the user's question - have you answered it if not please perform the next steps now. Please generate any more questions you might have and try the available functions. Use the entity store if you have found identifiers in the data such as SKus, styles, body codes, Ones, orders, materials etc.",
                    }
                )

            # cls._slack.try_typing(channel_context)

            MSG = len(json.dumps(cls._messages, cls=NpEncoder, default=str))
            logger.info(
                f"<<<<<<< Looping with messages at index {idx} with message count {len(cls._messages)} of total sizes {MSG} and token usage {response.usage} >>>>>>>>"
            )
            logger.debug(
                f"Functions loaded are {list(cls._active_function_callables.keys())}"
            )
            tokens += response.usage.total_tokens

            if response.choices[0].finish_reason == "stop":
                logger.info(f"Completed with total token usage {tokens}")
                break

        res.utils.logger.info(
            "---------------------------------------------------------------------------"
        )
        res.utils.logger.info(response_message.content)
        res.utils.logger.info(
            "---------------------------------------------------------------------------"
        )

        if response_callback is not None:
            # its good to send back the message before dumping auditing
            # TODO - we can thread_ts=thread_ts here if we choose to replay in thread on some channels
            response_callback(
                response_message.content or "There were no data found",
                thread_ts=thread_ts,
            )

        cls._dump(
            name=f"run{res.utils.res_hash()}",
            question=question,
            response=response_message.content,
            session_key=session_key,
            user_context=user_context,
            channel_context=channel_context,
        )

        if cls._user_session:
            cls._user_session.add(f"User asked {question}")
            cls._user_session.add(f"Agent responded: {response_message.content}")

        return response_message.content or "There were no data found"

    def enumerate_files_to_post_to_user(cls, files: typing.List[str], limit: int = 5):
        """
        You can request to make s3 files available to the user by parsing out any references in textual content and passing them to this function
        A limit reminds you that as these will be posted to media such as slack, it is advisable to not post to many at a time
        **Args**
           files: pass a list of s3 files found in content
           limit: limit the amount of those files that can be send to the user
        """
        # this function simply formats the response for now send we generically check for files extracted in this format
        # we could do some checks or processing here
        return {"files": files[:limit]}

    def check_should_post_file(cls, function_response, channel, thread_ts=None):
        """
        Working out a flow - here the idea is that if functions return instructions to post files in we can just generically deal with this here.
        we hard coded the api function caller helper to still only return json rather than dealing with arb types
        the files are a special case where we save the files on s3 and then serve -> direct to bytes would be good to but this works for now
        """
        try:
            # TODO: model this would a proper return type for typed behaviors
            if not isinstance(function_response, dict):
                return
            files = function_response.get("files")
            # dont past the same file twice
            # files = [f for f in files if f not in cls._posted_files]
            if files and channel:
                res.utils.logger.debug(
                    f"Checking for files to post - {files} to channel {channel} "
                )

                cls._slack.post_s3_files(files, channel=channel, thread_ts=thread_ts)
                res.utils.logger.info(f"Sent files {files} to channel = {channel}")
                cls._posted_files += files
        except Exception as ex:
            res.utils.logger.warn(f"Problem checking files to post {ex}")

    def _dump(
        cls, name, question, response, session_key, user_context, channel_context
    ):
        """
        dump the session to the store
        """
        logger.debug(f"Dumping session")

        # todo add attention comments which are useful

        record = InterpreterSession(
            plan=cls.PLAN,
            session_key=session_key,
            name=name,
            question=question,
            response=response or "",
            # dump the function descriptions that we used - some may have been purged...
            function_usage_graph=json.dumps(
                [f.function_dict() for f in cls._active_functions]
            ),
            audited_at=res.utils.dates.utc_now_iso_string(),
            messages=json.dumps(cls._messages),
            user_id=user_context or "",
            channel_context=channel_context or "",
            text="",
        )

        cls._audit_store.add(record)


# Extremely Brief
# Terse
# Minimalist
# Succinct
# Brief
# Concise
# Compact
# Focused
# Moderate
# Detailed
# Comprehensive
# In-depth
# Extensive
# Elaborate
# Exhaustive
# Ultra-detailed
# All-Encompassing
# Encyclopedic
# Unabridged and Expansive
# Painstakingly Detailed
