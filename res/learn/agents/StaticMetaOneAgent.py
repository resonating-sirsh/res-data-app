from res.media.parsers.TechpackParser import TechpackParser
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
    AbstractEntity,
    NpEncoder,
)
import res


class StaticMetaOneAgent:
    PLAN = """
    use the tech pack parser to answer questions
    """

    def __init__(self, allow_functions=True):
        pdf_doc = (
            "s3://res-data-platform/doc-parse/bodies/EP-6000-V0/input_tech_doc.pdf"
        )
        dxf_file = "s3://res-data-platform/doc-parse/bodies/EP-6000-V0/input.dxf"

        res.utils.logger.info(f"Loading agent for {pdf_doc=}, {dxf_file=}")
        # we will probably refactor tech pack parser to make it an agent in its own right
        self.tp = TechpackParser(pdf_doc, key="EP-6000-V0", dxf_file=dxf_file)
        self.tp.restore()
        res.utils.logger.info(f"agent loaded")

    def __call__(
        self,
        question,
        user_context=None,
        channel_context=None,
        thread_ts=None,
        response_callback=None,
        **kwargs,
    ):
        # reload whatever is being processed even if it takes a second
        self.tp.restore()

        response = self.tp.ask(
            question,
            prompt="""respond in clear text in the language the question is asked. Only if there is lots of data, use suitable structured formats like markdown or json if the data have structure.
                     if asked to describe an image. see if the image is already summarized in the garment technical details and use the sketch summary. otherwise take a look at the image (once) and describe it in detail.
                     If asked for a file or link to a file please replay with a link to the file on s3 as found in the data and use the Json format below to describe the files.
                     
                     **Format IF and only IF asked for Files or links to files  (dont use markdown block fencing)**
                     {
                         "files: [ "s3://file/1.example", "s3://file/1.example"]
                     }
               
            """,
            response_format=None,
            channel_context=channel_context,
            user_context=user_context,
        )

        if response_callback:
            response_callback(response)

        return response
