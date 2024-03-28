from res.learn.agents.builder.AgentBuilder import ResAgent
from res.learn.agents.builder.utils import ask
from res.flows.meta.ONE.instantiate.model import BodyIntake
import json
import res


# one way to create an agent is to sub class - we will show the declarative way too with lib functions
class BodyIntakeAgent(ResAgent):
    """
    Provides an example of extending res agent via subclassing and not config.
    default config and prompts are used. You can override the config and/or prompt
    Here the class adds whatever functions are needed while a declarative mode loads functions dynamically
    """

    def __init__(self, config=None, prompt_override=None):
        super().__init__(config, prompt_override=prompt_override)

        self.add_function(self.search_body_information)

        ##add slack search and graph search for concepts and things related to it

    def search_body_information(
        self,
        body_code: str,
        question: str = None,
        body_version: int = 0,
        context: str = None,
    ):
        """
        Use this function to ask questions relating to a body onboarding development from initial intake of tech packs and pattern files to ongoing development discussion.
        You should ask a detailed question and supply at least the body code.
        Bodies are Fashion Garment patterns and construction information needed to physically make garments.
        If asked for visual descriptions review the front and back sketches and any image descriptions. If asked for files or images directly simply return the S3 file links which can be used by the user.

        **Args**
          body_code: a body to search for
          body_version: a specific body version if known - defaults to 0 fpr body intake
          question: ask a question about the body
          context: you should say why you are calling this function
        """

        question = question or "Please describe the garment in detail"

        res.utils.logger.debug(
            f"I will search for {body_code=} {body_version=} | {question=} ****{context=}****"
        )

        # load the intake wrapper for this body - todo add body version thing - raise exception not exists (but Vx exists)
        m = BodyIntake(body_code)

        prompt = f"""
        Below are data you can use to answer the users question. Please be exhaustive in your response.
        
        **Question**
        ```
        {question}
        ```
        
        **Data**
        ```
        {json.dumps(m.tech_pack_output)}
        ```
        
        If you are asked for links to files, you can respond with a json format. This format can include commentary but should have a top level element called files.
        For example a suitable format for file responses would be 
        ```
          {
              'comments': "some commentary",
              'files': ["s3://link/to/file"]
          }
        ```
        
        But in general you do not need to respond in a structured format if the question does not warrant it
            
        """

        return ask(prompt)
