from langchain.agents import initialize_agent
from langchain.chat_models import ChatOpenAI
from langchain.chains.conversation.memory import ConversationBufferWindowMemory
from res.learn.agents.data.EntityDataStore import EntityDataStore
import res

# example text

# POC: f""" List things that look like entities, codes or identifiers in the sentence {q}. Provide the list with just the values as csv  """


def make_question_prompt1(text, question):
    """
    text = "This is a sentence that mentions KT-2043 in the context of an order 10001299 and there is a style reference KT-2043 CTSPS KITNNT PTZ12. The Body version is 5 and there were some issues with the older version 4 where the elastic was missing"

        question:
        - name the order|body|style|sku mentioned in the text
        -  What was the Color and Size of the the style mentioned in the text.
        - Can you tell me what what the previous version of the Body mentioned in the text and what was the problem with.
        - What is the Name of the body that is referenced in the text.
        - Describe the relationships between the entities in RDF triples. Provide only the RDF triples in the response without any blurb.
    """
    return f"""There are a number of entities mentioned in the sentence {text}. {question}.
        If you dont know what the entity types are, use the entity resolver tool to determine the entity types by passing the identifiers or codes only and getting back a mapping of the identifiers to their types"""


def get_entity_aware_chatty_agent():
    chatty_llm = ChatOpenAI(temperature=0, model_name="gpt-4")
    tools = [EntityDataStore().as_tool()]

    conversational_memory = ConversationBufferWindowMemory(
        memory_key="chat_history", k=5, return_messages=True
    )

    # initialize agent with tools
    agent = initialize_agent(
        agent="chat-conversational-react-description",
        tools=tools,
        llm=chatty_llm,
        verbose=True,
        max_iterations=3,
        early_stopping_method="generate",
        memory=conversational_memory,
    )
    return agent


def summarize_pod_logs(uri, agent):
    """
    specify a folder of logs - as there is a token limit choose a folder with one or only a few logs for a pod instance
    """

    d = res.connectors.load("s3").read_logs_in_folder(uri)
    text = "\n".join(d)
    ag = agent or get_entity_aware_chatty_agent()
    a = ag(
        f"""There are a number of entities as well as problems like warnings, errors and contract failures mentioned in the text {text}. Summarize the text with reference to the entities and any problems.
        If you dont know what the entity types are, use the entity resolver tool to determine the entity types by passing the identifiers or codes only and getting back a mapping of the identifiers to their types"""
    )
    return a
