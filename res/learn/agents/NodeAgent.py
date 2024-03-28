import json
import pandas as pd

# you have install lanchain and its deps and set your OPEN AI key
from langchain.agents import ZeroShotAgent, AgentExecutor
from langchain import LLMChain
from langchain.chat_models import ChatOpenAI


from res.utils.env import RES_DATA_BUCKET

DEFAULT_RENDER_INSTRUCTION = " If the answer contains tabular data please represent the tabular data in aligned table markdown format inside triple back quotes."


def make_node_agent_from_tools(
    tools, render_context=DEFAULT_RENDER_INSTRUCTION, model="gpt-4", temperature=0.09
):
    # If you are asked about orders you can use the sku as an identifier. this is needed because the queries that come back are undirected without that e.g. * results so this helps to refine
    RES_CONTEXT = """This is always asked in the context of our company Resonance. We sell garments in different styles and those styles have special fittings that we call bodies and they come in different materials and colors (colors are the artwork that the brand uses in their design).
        We run the business through a supply chain described in terms of flows and nodes. If there is a problem in the supply chain we call it a contract violation or defect and if we need to repair something we call it a healing or a reproduce.
        When we make something we call it a ONE.  Ones and production requests refer to the same thing.  Instead of saying ones say production requests.
        A production request can also be in ONE which means that its make node is called one.
        """
    # If you are asked about orders you can use the sku as an identifier. this is needed because the queries that come back are undirected without that e.g. * results so this helps to refine
    prefix = f"""
        Answer the question in the context of any entities you observe in the question or in the context using all the available tools. 
        
        {RES_CONTEXT}
        Follow this strategy to answer the question and use all the context provided:
        - You should get context by running the stats and data tool first if you can.
        - Then expand any entity codes into their component terms, and pass all terms to the entity resolution tool.
        - The Further details tool should be used if and only if there there is no information available in the other tools. Do not pass identifiers and codes to this tool. Only pass proper nouns and questions.
        Lets takes this step by step.
        You have access to the following tools:"""

    # I append a render instruction after the input as though its parse of the question with care for formatter. probabaly a better way
    suffix = (
        """Begin! Give terse answers.
        Question: {input} - """
        + DEFAULT_RENDER_INSTRUCTION
        + """
        {agent_scratchpad}"""
    )

    if not isinstance(tools, list):
        tools = [tools]

    prompt = ZeroShotAgent.create_prompt(
        tools,
        prefix=prefix,
        suffix=suffix,
        input_variables=["input", "agent_scratchpad"],
    )
    llm = ChatOpenAI(model_name=model, temperature=temperature)
    llm_chain = LLMChain(llm=llm, prompt=prompt)
    tool_names = [tool.name for tool in tools]
    agent = ZeroShotAgent(llm_chain=llm_chain, allowed_tools=tool_names)
    agent_executor = AgentExecutor.from_agent_and_tools(
        agent=agent, tools=tools, verbose=True
    )

    return agent_executor
