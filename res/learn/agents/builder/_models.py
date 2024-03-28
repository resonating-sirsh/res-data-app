from pydantic import BaseModel
import typing


class FunctionInfo(BaseModel):
    name: str
    reason_for_choosing: str
    rating_percent: float
    # specific named factories
    factory: typing.Optional[str]
    # make this an enum APi | VectorStore | ColumnarStore | Agent | Lib
    function_class: typing.Optional[str]


class UsefulFunctions(BaseModel):
    functions: typing.List[FunctionInfo]


class AgentConfig(BaseModel):
    name: str
    prompt: str
    namespace: str = "default"
    functions: typing.List[FunctionInfo]
    model: typing.Optional[str] = None
    response_format: dict = None
    include_image_inspection: bool = False
    include_function_search: bool = False
    allow_function_broadcast: bool = False

    def default(
        prompt=None,
        name="My Agent",
        functions=None,
    ):
        prompt = prompt or "Answer the question using the functions provided. "
        return AgentConfig(name=name, prompt=prompt, functions=functions or [])
