from res.learn.agents.builder import (
    typing,
    EntityDataStore,
    Caches,
    AbstractEntity,
    FunctionFactory,
    describe_function,
    update_function_index,
    FunctionManager,
    AgentConfig,
    BaseModel,
)
import res
import json
import openai


DEFAULT_MODEL = "gpt-4-1106-preview"
CPUS = 4


def _ask(tuple):
    """
    generally call a function using (q,f) to load a function from a store and pose a question
    """
    fm = FunctionManager()
    q, f = tuple
    # because...
    f = f.split(".")[-1]
    fd = fm[f]
    return {"function": f, "question": q, "response": fd.function(q) if fd else None}


class ConversationResponseContext(BaseModel):
    """
    for wrapping the slack or similar bits
    """

    thread_ts: typing.Optional[str] = None
    user: typing.Optional[str] = None
    callback: typing.Callable
    channel_context: typing.Optional[str]


class ResAgent:
    def __init__(cls, config: AgentConfig, prompt_override=None):
        cls._config = config or AgentConfig.default(prompt=prompt_override)
        cls._function_manager = FunctionManager()
        cls._functions = []
        cls._messages = []
        cls._response_format = (
            None  # cls._config.response_format or {"type": "json_object"}
        )
        cls._model = (cls.config.model if cls.config else None) or DEFAULT_MODEL
        cls._tokens = 0
        cls._entity_store = EntityDataStore(AbstractEntity, cache_name=Caches.QUEUES)
        cls.build_functions()

    @property
    def config(self):
        return self._config

    @property
    def entity_namespace(self):
        return self.config.namespace

    @property
    def entity_name(self):
        return self.config.name

    @property
    def description(self):
        return self.config.prompt

    def entity_search(cls, keys: typing.List[str]):
        """
        Use the entity search to lookup entities such as bodies, styles, orders, materials and ONEs. They take formats such as these examples
        - Bodies are of the form XX-1234 e.g. KT-2011
        - Style codes are three part (Body Material Color) such as KT-2011 CHRST WHTTIY (a fourth Size component such as ZZ123 makes up a SKU)
        - orders have codes such as XX-123456

        **Args**
        keys: supply one or more keys to the entity store
        """
        return cls._entity_store(keys)

    def _setup_conversation(
        cls,
        question,
        user_context=None,
        channel_context=None,
        thread_ts=None,
        response_callback=None,
        as_json_response=False,
        **kwargs,
    ):
        """
        user_context=username,
        channel_context=channel_name,
        response_callback=say,
        thread_ts=reply_ts,

        response_callback should be sent from slack context. you can test locally by creating

        def callback_test(message, thread_ts, **kwargs):
            print('*******************')
            print(message, thread_ts)
            print('*******************')

        a('What are the piece names for EP-6002', response_callback=callback_test, thread_ts=1223)

        for true callbacks this will be linked to a slack channel
        """
        cls._posted_files = []
        cls._track_single_call = []
        cls._question = question
        cls._messages = [
            {
                "role": "system",
                "content": (
                    cls.config.prompt
                    if cls.config
                    else "Answer the question using provided functions"
                ),
            },
            {"role": "user", "content": question},
            {
                "role": "system",
                "content": f"Please note the current date is {res.utils.dates.utc_now()} so you should take that into account if asked questions about time",
            },
        ]
        if "json" in str(cls._response_format) or as_json_response:
            cls._messages.append(
                {
                    "role": "user",
                    "content": "Use a suitable JSON response format unless otherwise specified",
                }
            )
        if response_callback is not None:
            return ConversationResponseContext(
                callback=response_callback,
                thread_ts=thread_ts,
                channel_context=channel_context,
                user_context=user_context,
            )
        return None

    def broadcast_questions(
        cls, questions: typing.List[str], functions: typing.List[str], **kwargs
    ):
        """
        if there are multiple search functions and you want to ask multiple questions (full sentence format) to multiple functions,
        you can simply call this one function and pass the list of questions and the list of functions.
        It is recommended to pass many questions to many functions at the same time and the stores will provide data if relevant.
        You will be provided a compiled answered showing which functions provide which response

        **Args**
          questions: one or more full length questions to ask
          functions: a list of functions to call with the question - each function is assumed to take a question like parameter but the name of that parameter is incidental
        """
        if isinstance(questions, str):
            questions = [questions]
        if isinstance(functions, str):
            functions = [functions]

        res.utils.logger.debug(f"BROADCASTING {questions=}, {functions=}, {kwargs}")
        import multiprocessing

        cross_product = [(x, y) for x in questions for y in functions]
        flat_product = [(questions, f) for f in functions]

        with multiprocessing.Pool(processes=CPUS) as pool:
            result = pool.map(_ask, flat_product)

        return result

    def invoke(cls, function_call):
        """
        wrapper to invoke the function and add the data to response
        """

        # we merged the function managers functions with any we add
        fn = cls._functions[function_call.name].function
        args = function_call.arguments
        res.utils.logger.info(f"fn={fn}, args={args}")

        args = json.loads(args) if isinstance(args, str) else args
        """
        handle function call
        """

        data = fn(**args)
        res.utils.logger.debug(f"Response: {data}")
        cls._messages.append(
            {
                "role": "function",  # <- roll is a function result
                "name": f"{str(function_call.name)}",
                "content": json.dumps(data, default=str),
            }
        )

        return data

    def __call__(cls, question, limit=10, response_callback=None, **kwargs):
        return cls.run(
            question, limit=limit, response_callback=response_callback, **kwargs
        )

    def run_search(cls, question: str, **kwargs):
        """
        Query the agent with a detailed question

        **Args**
            question: the question to ask the agent - be detailed
        """
        return cls.run(question)

    def run(cls, question, limit=10, as_json_response=False, **kwargs):
        response_format = None if not as_json_response else {"type": "json_object"}
        res.utils.logger.debug(question)
        response_context = cls._setup_conversation(
            question, as_json_response=as_json_response, **kwargs
        )
        res.utils.logger.debug(
            f"Looping after setting up conversation - {response_context=}"
        )
        for idx in range(limit):
            # this function alias is important because we match the name in the store to the name we send LLM
            functions_desc = [f.function_dict() for f in cls._functions.values() if f]
            res.utils.logger.debug(f"Running with functions {functions_desc}")
            response = openai.chat.completions.create(
                model=DEFAULT_MODEL,
                messages=cls._messages,
                functions=functions_desc if cls._functions else None,
                response_format=response_format or cls._response_format,
                function_call="auto" if cls._functions else None,
            )
            response_message = response.choices[0].message
            res.utils.logger.debug(response_message)
            function_call = response_message.function_call
            if function_call:
                cls.invoke(function_call)
            cls._tokens += response.usage.total_tokens

            if response.choices[0].finish_reason == "stop":
                res.utils.logger.info(f"Completed with total token usage {cls._tokens}")

                break

        if response_context is not None:
            response_context.callback(
                response_message.content or "There were no data found",
                thread_ts=response_context.thread_ts,
            )
            cls.check_should_post_file(
                response_message.content, response_context=response_context
            )

        return response_message.content or "There were no data found"

    def dump(cls, uri=None, register=False):
        """
        Dumps the config for this agent -the agent can later be restored
        """
        uri = (
            uri
            or f"s3://res-data-platform/ask-one/agents/{cls.config.namespace}/{cls.config.name}.yaml"
        )
        res.utils.logger.debug(f"dumping {uri}...")

        if register:
            context = f"{cls.config.prompt}. Has function searched with functions {json.dumps(cls.config.functions,default=str )}"
            update_function_index(
                cls, summary=context, short_description=cls.config.prompt
            )
            fd = cls.as_function_description()
            # register in the cache
            res.connectors.load("redis")["OBSERVE"]["FUNCTIONS"][fd.name] = fd

        return res.connectors.load("s3").write(
            uri,
            cls.config.dict(),
        )

    def restore(name, namespace="default", uri=None, **options):
        """
        Restore the function from persisted config
        the namespace and name by convention determines but the uri can override
        options allow for changing some of the options on the way in e.g. flagging some of the booleans to try with other options
        """
        uri = uri or f"s3://res-data-platform/ask-one/agents/{namespace}/{name}.yaml"
        data = res.connectors.load("s3").read(uri)
        data.update(options)
        return ResAgent(AgentConfig(**data))

    @property
    def function_names(cls):
        return [f.name for f in cls.config.functions] if cls.config else []

    def add_function(cls, function: typing.Callable) -> str:
        """
        add a function to callable list
        """
        f = describe_function(function)
        res.utils.logger.debug(f"adding {f.name}")
        cls._functions[f.name] = f
        return f.name

    def build_functions(cls) -> typing.List[str]:
        """
        Build the functions defined in config. Always add the entity store and then any other optional built-ins
        return the list of function names/keys
        """
        res.utils.logger.info(f"building {len(cls.function_names)} functions...")
        # in case any confusion use the name in the function dict we pass to GPT - there may be collisions so we need to work on the method
        cls._functions = {fn: cls._function_manager[fn] for fn in cls.function_names}

        cls._functions["entity_search"] = describe_function(cls.entity_search)

        if cls._functions:
            if cls.config.include_image_inspection:
                res.utils.logger.debug(f"adding also entity_search")
                cls._functions["describe_visual_image"] = describe_function(
                    cls.describe_visual_image
                )
            if cls.config.include_function_search:
                res.utils.logger.debug(f"adding also run_function_search")
                cls._functions["run_function_search"] = describe_function(
                    cls.run_function_search
                )
            if cls.config.allow_function_broadcast:
                res.utils.logger.debug(f"adding also broadcast_questions")
                cls._functions["broadcast_questions"] = describe_function(
                    cls.broadcast_questions
                )

        return list(cls._functions.keys())

    def __repr__(cls):
        return str(cls.config)

    def run_function_search(cls, questions: typing.List[str], max_functions: int = 3):
        """Use this function to search for more functions if you cannot solve the problem with the functions you already have
        you may need to ask a different question than the one the user asked by inferring what types of functions you need and ask for those functions
        Use the function namespaces like `slack`, `queue`, `code` and the function types like API, Vector, Columnar to make the right choices in the context of the question

        **Args**
         questions: one or more questions (more preferred) used to search for functions - be sure to pass the full context of the users preference
         max_functions: how many functions to consider adding - you may only need 1 but you could try more
        """

        results = cls._function_manager.function_search(
            questions, max_functions=max_functions
        )
        res.utils.logger.debug(results)
        new_function_names = [f["name"] for f in results]

        new_functions = {
            fn: cls._function_manager[fn] for fn in [f for f in new_function_names]
        }

        cls._functions.update(new_functions)

        return {
            "result": f"I added the following new functions which you can now call: {new_function_names}"
        }

    def describe_visual_image(
        cls,
        url: str,
        question: str = "describe the image you see in exhaustive detail in the context of Garment manufacturing processes",
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

    def as_function_description(cls, context=None, name=None, weight=0):
        """
        Pass in the name and context - this is used to create a function description that can be saved and rehydated when registering functions
        """
        context = (
            context
            or f"Provides context about the type [{cls.entity_name}] - ask your full question as per the function signature. {cls.description}"
        )

        return describe_function(
            cls.run_search,
            alias=name or f"run_search_{cls.config.namespace}_{cls.config.name}",
            augment_description=context,
            weight=weight,
            # this is a bit lame but doing it for now. we need a proper factory mechanism
            factory=FunctionFactory(
                # for abstract stores this is the name but not in general
                name="run_search",
                weight=weight,
                partial_args={
                    "store_type": str(cls.__class__),
                    "store_name": f"{cls.entity_namespace}.{cls.entity_name}",
                },
            ),
        )

    def check_should_post_file(
        cls, function_response, response_context: ConversationResponseContext = None
    ):
        """
        Working out a flow - here the idea is that if functions return instructions to post files in we can just generically deal with this here.
        we hard coded the api function caller helper to still only return json rather than dealing with arb types
        the files are a special case where we save the files on s3 and then serve -> direct to bytes would be good to but this works for now
        """

        try:
            slack = res.connectors.load("slack")
            if not isinstance(function_response, dict):
                from res.observability.dataops import parse_fenced_code_blocks

                function_response = parse_fenced_code_blocks(
                    function_response, select_type="json"
                )
                if not function_response:
                    return

                function_response = function_response[0]

            files = function_response.get("files")
            # dont past the same file twice
            # files = [f for f in files if f not in cls._posted_files]
            if files and response_context:
                if isinstance(files, str):
                    files = [files]
                files = list(set(files))
                res.utils.logger.debug(
                    f"Checking for files to post - {files} to channel {response_context.channel_context} "
                )

                slack.post_s3_files(
                    files,
                    channel=response_context.channel_context,
                    thread_ts=response_context.thread_ts,
                )
                res.utils.logger.info(
                    f"Sent files {files} to channel = {response_context.channel_context}"
                )

        except Exception as ex:
            res.utils.logger.warn(
                f"Problem checking files to post {ex} when parsing from {function_response}"
            )
