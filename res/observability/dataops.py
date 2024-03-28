"""
inspection is one of the main objectives of FunkyPrompt so we need to refine this guy in particular
for now its just a sketch
"""

import inspect
import typing
import re
from pydantic import Field, BaseModel
import json
import importlib
import pkgutil
import sys
from enum import Enum
import stringcase
import json
from functools import partial
from res.utils import res_hash
import res


def parse_fenced_code_blocks(input_string, try_parse=True, select_type="python"):
    """
    extract code from fenced blocks - will try to parse into python dicts if option set
    """
    pattern = r"```(.*?)```|~~~(.*?)~~~"
    matches = re.finditer(pattern, input_string, re.DOTALL)
    code_blocks = []
    for match in matches:
        code_block = match.group(1) if match.group(1) else match.group(2)
        if code_block[: len(select_type)] == select_type:
            code_block = code_block[len(select_type) :]
        code_block.strip()
        if try_parse and select_type == "json":
            code_block = json.loads(code_block)
        code_blocks.append(code_block)
    return code_blocks


def split_string_with_quotes(string):
    pattern = r'"([^"]*)"'
    quoted_substrings = re.findall(pattern, string)
    placeholder = "<<<<>>>>"
    modified_string = re.sub(pattern, placeholder, string)
    split_parts = modified_string.split()
    result = []
    for part in split_parts:
        if placeholder in part:
            # Replace the placeholder with the quoted substring
            part = part.replace(placeholder, quoted_substrings.pop(0))
        result.append(part)


def call_api(endpoint, verb="get", **kwargs):
    """

    simple version tested for gets -> we will create a partial factory on the endpoint

    call_api('meta-one/meta-one', sku='LH-3003 LN135 CREABM 4ZZLG').json()


    ensure secrets
    from res.utils.secrets import secrets
    secrets.get_secret('RES_META_ONE_API_KEY')

    """
    import os
    import requests

    KEY = os.environ["RES_META_ONE_API_KEY"]
    headers = {"Authorization": f"Bearer {KEY}", "Content-type": "application/json"}

    f = getattr(requests, verb)

    # tested for gets
    response = f(
        f"https://data.resmagic.io/{endpoint.lstrip('/')}",
        headers=headers,
        params=kwargs,
    )

    # for now going to assume its a json response always
    try:
        return response.json()
    except:
        # try to infer some action for content types
        content_type = response.headers.get("Content-Type")
        res.utils.logger.warn(
            "Inferring content - this is limited and experimental - write data as temp files on s3"
        )
        if "image" in content_type:
            # e.g. image/jpeg, image/png - not handling everything
            ftype = content_type.split("/")[-1]
            # get the image bytes and save to temp file - just write all as png for now -
            file = f"s3://res-data-platform/temp-files/{res_hash()}.png"
            # write the bytes to file
            res.connectors.load("s3").write_image_bytes(file, response.content)
            return {
                # return file handle for consumer to do something with e.g. send as slack attachment
                "comment": "The files are saved to a location that allows them to be relayed to the user via slack or other means if required - no other action is needed",
                "files": [file],
            }

        return {"unable to get a response from the API": response.content}
        # raise NotImplementedError(
        #     f"Response content types should be handled properly if they are not json responses in the current implementation. got response headers {response.headers}"
        # )


class CallableModule(BaseModel):
    name: str
    namespace: typing.Optional[str]
    fullname: typing.Optional[str]
    interval_hours: typing.Union[typing.Any, None] = None
    interval_minutes: typing.Union[typing.Any, None] = None
    interval_days: typing.Union[typing.Any, None] = None
    options: typing.Optional[dict] = None


class FunctionFactory(BaseModel):
    """
    this is used to reload functions and partially eval them for different contexts
    With the function description and factory we can satisfy the LLM interface and also generate functions over stores
    """

    name: str
    module: typing.Optional[str] = None
    # often contains store_name
    partial_args: typing.Optional[dict] = Field(default_factory=dict)


class FunctionDescription(BaseModel):
    """
    Typed function details for passing functions to LLMs/
    """

    name: str
    description: str
    parameters: dict
    raises: typing.Optional[str] = None
    returns: typing.Optional[str] = None
    function: typing.Any = Field(exclude=True)
    # serialize function ref. we need to load something with partial args
    factory: typing.Optional[FunctionFactory] = None
    weight: float = 0
    # function namespace and category or "subjects"

    @classmethod
    def restore(cls, data, weight=0, alias=None):
        """
        this is a temporary sketch - it would probably make more sense for the ops utils e.g. inspector to allow for restoring function descriptions
        will refactor as the ideas converge
        """
        from res.observability.io import open_store
        from res.observability.dataops import load_op

        if isinstance(data, str):
            data = json.loads(data)

        factory = data["factory"]
        # temp - for now all functions will be restored as store functions
        if factory["name"] == "api_call":
            # proxy the function
            args = factory["partial_args"]
            fn = partial(call_api, endpoint=args["endpoint"], verb=args["verb"])
            # set the function
            data["function"] = fn
            data["weight"] = weight
            # call with all the stuff
            return FunctionDescription(**data)

        elif factory["name"]:  #: == "store_search":
            """
            Here we assume a certain pattern for the factory
            #agents look like this
            partial_args': {'store_type': "<class 'res.learn.agents.builder.AgentBuilder.ResAgent'>", 'store_name': 'default.queue_agent'}},

            """
            store = factory["partial_args"]["store_name"].split(".")
            alias = alias or f"run_search_{store[0]}_{store[1]}"
            store = {"type": None, "namespace": store[0], "name": store[1]}

            # bit hacky - we need a general way to load stores or functions and its not fleshed out yet
            if "AgentBuilder.ResAgent" in factory["partial_args"]["store_type"]:
                from res.learn.agents.builder.AgentBuilder import ResAgent

                store["type"] = "AgentBuilder.ResAgent"
                store = ResAgent.restore(name=store["name"])

                return store.as_function_description()
                # restore from s3 for now the config
            else:
                store = open_store(
                    **store,
                    embedding_provider=factory["partial_args"].get("embedding"),
                )

            return store.as_function_description(name=alias, weight=weight)
        # implement other store
        else:
            # assume its an op
            return FunctionDescription(load_op(factory["name"]), weight=weight)

    def from_openapi_json(url_or_json: typing.Union[str, dict], endpoint, verb="get"):
        """
        Get the openapi spec, endpoint and make a function

        **Args**
            url_or_json: the spec, can be the url to the json, the json text or python dict
            endpoint: filter the endpoint e.g '/meta-one/meta-one
            verb: filter the verb
        """
        import requests

        if "http://" or "https://" in url_or_json:
            url_or_json = requests.get(url_or_json).json()
        elif isinstance(url_or_json, str):
            url_or_json = json.loads(url_or_json)

        def get_component(comp):
            # assume just a schema type
            s = comp.split("/")[-1]
            return s

        # assert leading and trailing / to save user stress
        schema = url_or_json["paths"][endpoint][verb]
        schema["name"] = stringcase.snakecase(schema["summary"]).replace("__", "_")

        # treat params, and renaming
        def _treat(d):
            d.update(d["schema"])
            d["description"] = d.get("description", d.get("title", ""))
            # TODO we can pop the required from here and put them into a required list up one level
            return {k: v for k, v in d.items() if k in ["description", "type"]}

        schema["parameters"] = {p["name"]: _treat(p) for p in schema["parameters"]}
        # best practice to describe the function but just in case...
        schema["description"] = schema.get("description") or schema["name"]

        factory = FunctionFactory(
            name="api_call", partial_args={"verb": "get", "endpoint": endpoint}
        )

        # the callable is like this
        args = factory.partial_args
        fn = partial(call_api, endpoint=args["endpoint"], verb=args["verb"])

        return FunctionDescription(**schema, function=fn, factory=factory)

        #

    def pop_object_types(cls, d):
        """
        this is prompt engineering to describe function calls
        """
        objects = {}
        desc = ""
        for param, param_description in d.items():
            # this is the schema type check - should also type your shit but if you dont ill assume its a string
            param_type = param_description.get("type", str)
            if isinstance(param_type, dict):
                objects[param] = param_type
                # replace the description with something that points to the schema
                # i tried just leaving this but it seems more reliable to prompt in the function desc.
                # at least at the time of writing
                d[param] = {
                    "type": "object",
                    "description": "This is a complex Pydantic type who's schema is described in the function description",
                }

        for param_name, v in objects.items():
            desc += f"The parameter [{param_name}] is a Pydantic object type described below: \n"
            desc += f"json```{json.dumps(v)}```\n"
        return desc

    def function_dict(cls, function_alias=None):
        """
        describe the function for the LLM
        this is the openAI flavour

        a function alias is useful in case of name collisions
        """
        d = cls.dict()
        object_descriptions = cls.pop_object_types(d["parameters"])
        return {
            "name": function_alias or d["name"],
            "description": f"{d['description']}\n{ object_descriptions}",
            "parameters": {"type": "object", "properties": d["parameters"]},
        }


def is_pydantic_type(t):
    """
    determine if the type is a pydantic type we care about
    """

    try:
        from pydantic._internal._model_construction import ModelMetaclass
    except:
        from pydantic.main import ModelMetaclass

    return isinstance(t, ModelMetaclass)


def describe_function(
    function: typing.Callable,
    add_sys_fields=False,
    augment_description=None,
    factory: FunctionFactory = None,
    alias: str = None,
    weight: int = 0,
) -> FunctionDescription:
    """
    Used to get the description of the method for use with the LLM
    This is only setup for a few test cases and not for general types
    assumes some Pydantic types or optional/primitives or returns object

    """
    type_hints = typing.get_type_hints(function)

    def python_type_to_json_type(python_type):
        """
        map typing info
        todo: implement enums
        diagram_request
        """

        if python_type == int:
            return "integer"
        elif python_type == float:
            return "number"
        elif python_type == str:
            return "string"
        elif python_type == bool:
            return "boolean"
        # for pydantic objects return the schema
        if is_pydantic_type(python_type):
            return python_type.schema()

        if inspect.isclass(python_type) and issubclass(python_type, Enum):
            # assume uniform types
            delegate_type = type(list(python_type)[0].value)
            return python_type_to_json_type(delegate_type)
        else:
            return "object"

    def parse_args_into_dict(args_text):
        """
        parse out args with type mapping for the agent
        TODO: support more complex typing e.g. options and unions etc...
        """
        args_dict = {}

        for line in args_text.splitlines():
            parts = line.strip().split(":")
            if len(parts) == 2:
                param_name = parts[0].strip()
                param_description = parts[1].strip()
                T = type_hints[param_name]
                if hasattr(T, "__args__"):
                    # this is just for the optional case - not general case
                    T = T.__args__[0]
                # TODO: handle optional types e.g Optional[str]
                args_dict[param_name] = {
                    # assuming the name of the type matches between args and doc string
                    "type": python_type_to_json_type(T),
                    "description": param_description,
                }
                # for enums adds the choices
                if inspect.isclass(T) and issubclass(T, Enum):
                    args_dict[param_name]["enum"] = [member.value for member in T]
        """
        we can optionally add system fields 
        """
        if add_sys_fields:
            args_dict["__confidence__"] = {
                "type": "string",
                "description": "Your confidence between 0 and 100 that calling this function is the right thing to do in this context",
            }
            args_dict["__parameter_choices__"] = {
                "type": "string",
                "description": "Explain why you are passing these parameters",
            }
        return args_dict

    docstring = function.__doc__
    parsed_sections = {}

    sections = re.split(r"\n(?=\s*\*\*)", docstring)

    for section in sections[1:]:
        match = re.match(r"\s*\*\*\s*(.*?)\s*\*\*", section)
        if match:
            section_name = match.group(1)
            section_content = re.sub(
                r"\s*\*\*\s*" + section_name + r"\s*\*\*", "", section
            ).strip()
            if section_name.lower() in ["args", "params", "arguments", "parameters"]:
                parsed_sections["parameters"] = parse_args_into_dict(section_content)

            else:
                parsed_sections[section_name.lower()] = section_content

    parsed_sections["description"] = sections[0].strip()
    if augment_description:
        parsed_sections["description"] += f"\n{augment_description}"
    parsed_sections["name"] = alias or function.__name__

    return FunctionDescription(
        **parsed_sections, function=function, factory=factory, weight=weight
    )


def list_function_signatures(module, str_rep=True):
    """
    the describe the signatures of the methods in the module
    """
    stringify = lambda s: re.findall(r"\((.*?)\)", str(s))[0]
    members = inspect.getmembers(module)
    functions = [member for member in members if inspect.isfunction(member[1])]
    function_signatures = {name: inspect.signature(func) for name, func in functions}
    if str_rep:
        return [f"{k}({stringify(v)})" for k, v in function_signatures.items()]
    return function_signatures


def list_functions(module=None, description=True):
    """
    the describe the signatures of the methods in the module
    """

    members = inspect.getmembers(module)
    functions = [member for member in members if inspect.isfunction(member[1])]

    if description:
        # because its a tuple we fetch the function to describe
        return [describe_function(f[-1]) for f in functions]

    return functions


def _get_module_callables(name, module_root):
    """
    an iterator for callable modules
    """
    MODULE_ROOT = f"{module_root}."
    fname = name.replace(MODULE_ROOT, "")
    namespace = f".".join(fname.split(".")[:2])
    for name, op in inspect.getmembers(
        importlib.import_module(fname), inspect.isfunction
    ):
        if name in ["generator", "handler"]:
            d = {
                "name": f"{namespace}.{name}",
                "fullname": f"{fname}.{name}",
                "namespace": namespace,
                "options": {} if not hasattr(op, "meta") else op.meta,
            }
            if hasattr(op, "meta"):
                # take non none values to override
                d.update({k: v for k, v in op.meta.items() if v is not None})
            yield CallableModule(**d)


def inspect_modules(
    module=None,
    filter=None,
) -> typing.Iterator[CallableModule]:
    """
    We go through looking for callable methods in our modules obeying some norms
    """
    path_list = []
    spec_list = []

    for importer, modname, ispkg in pkgutil.walk_packages(module.__path__):
        import_path = f"{module.__name__}.{modname}"
        if ispkg:
            spec = pkgutil._get_spec(importer, modname)
            importlib._bootstrap._load(spec)
            spec_list.append(spec)
        else:
            path_list.append(import_path)
            for mod in _get_module_callables(import_path):
                yield mod

    for spec in spec_list:
        del sys.modules[spec.name]


def inspect_modules_for_class_types(module=None, type_filter=None):
    """

    iterate over module for the type requested


    """
    path_list = []
    spec_list = []

    for importer, modname, ispkg in pkgutil.walk_packages(module.__path__):
        import_path = f"{module.__name__}.{modname}"
        if ispkg:
            spec = pkgutil._get_spec(importer, modname)
            importlib._bootstrap._load(spec)
            spec_list.append(spec)
        else:
            path_list.append(import_path)

            for name, obj in inspect.getmembers(importlib.import_module(import_path)):
                if inspect.isclass(obj) and issubclass(obj, type_filter):
                    yield import_path, name, obj

    for spec in spec_list:
        del sys.modules[spec.name]


def load_op(op, module):
    """
    much of this library depends on simple conventions so can be improved
    in this case we MUST be able to find the modules
     'X.modules.<NAMESPACE>.<op>'
    these ops currently live in the controller so a test is that they are exposed to the module surface
    or we do more interesting inspection of modules
    """

    return getattr(module, op)
