# import pandas as pd

# app = None


# import os
# from slack_bolt import App
# from slack_bolt.adapter.socket_mode import SocketModeHandler
# from langchain import LLMChain, OpenAI, PromptTemplate
# from langchain.chains.conversation.memory import ConversationBufferWindowMemory
# from res.utils.secrets import secrets
# import res
# import traceback

# secrets.get_secret("SLACK_ASK_ONE_APP_TOKEN")
# secrets.get_secret("SLACK_ASK_ONE_SIGNING_SECRET")
# secrets.get_secret("SLACK_ASK_ONE_BOT_TOKEN")
# secrets.get_secret("OPENAI_API_KEY")

# app = App(token=os.environ.get("SLACK_ASK_ONE_BOT_TOKEN"))

# print("APP INIT DONE")


# def prep():
#     template = """Assistant is a large language model trained by OpenAI.

#     Assistant is designed to be able to assist with a wide range of tasks, from answering simple questions to providing in-depth explanations and discussions on a wide range of topics. As a language model, Assistant is able to generate human-like text based on the input it receives, allowing it to engage in natural-sounding conversations and provide responses that are coherent and relevant to the topic at hand.

#     Assistant is constantly learning and improving, and its capabilities are constantly evolving. It is able to process and understand large amounts of text, and can use this knowledge to provide accurate and informative responses to a wide range of questions. Additionally, Assistant is able to generate its own text based on the input it receives, allowing it to engage in discussions and provide explanations and descriptions on a wide range of topics.

#     Overall, Assistant is a powerful tool that can help with a wide range of tasks and provide valuable insights and information on a wide range of topics. Whether you need help with a specific question or just want to have a conversation about a particular topic, Assistant is here to assist.

#     {history}
#     Human: {human_input}
#     Assistant:"""

#     prompt = PromptTemplate(
#         input_variables=["history", "human_input"], template=template
#     )

#     chatgpt_chain = LLMChain(
#         llm=OpenAI(temperature=0),
#         prompt=prompt,
#         verbose=True,
#         memory=ConversationBufferWindowMemory(k=2),
#     )

#     # output = chatgpt_chain.predict(human_input="I want you to act as a Linux terminal. I will type commands and you will reply with what the terminal should show. I want you to only reply with the terminal output inside one unique code block, and nothing else. Do not write explanations. Do not type commands unless I instruct you to do so. When I need to tell you something in English I will do so by putting text inside curly brackets {like this}. My first command is pwd.")
#     # print(output)
#     return prompt, chatgpt_chain


# prompt = None
# chatchain = None


# @app.message(".*")
# def any_find_one(message, say, logger):
#     # prompt, chatchain = prep()

#     # if message["text"] == "help":
#     #     say("https://www.youtube.com/watch?v=u3v7SMKwY6k")
#     # else:
#     try:
#         say("thinking...")
#         output = agent.run(message["text"])
#         #     print(output)
#         say(output)
#     except:
#         say("Sorry, could not figure that out")


# @app.command("/one_where_is_one")
# def find_one(message, say, **kwargs):
#     say("https://www.youtube.com/watch?v=u3v7SMKwY6k")


# @app.command("/fulfillment")
# def fulfillment_agent(ack, say, command):
#     ack()
#     from res.flows.api.rrm import ResilientResonanceMap

#     user_id = command["user_id"]
#     # channel_id = command["channel_id"]

#     fulfillment_agent = ResilientResonanceMap("sell", "order_items").as_agent()
#     say(
#         f"<@{user_id}>, im looking up the orders, give me a moment to do the thinking... "
#     )

#     response = fulfillment_agent.run(command["text"])

#     say(response)


# agent = None
# if __name__ == "__main__":
#     print("starting...")
#     try:
#         SocketModeHandler(
#             app, os.environ.get("SLACK_ASK_ONE_APP_TOKEN"), trace_enabled=True
#         ).start()

#         from res.learn.agents.tools.utils import zero_shot_agent_from_mode_agents

#         agent = zero_shot_agent_from_mode_agents()
#     except:
#         res.utils.logger.info(traceback.format_exc())


####
#
####

import os
from slack_sdk.web import WebClient
from slack_sdk.socket_mode import SocketModeClient

# Initialize SocketModeClient with an app-level token + WebClient
client = SocketModeClient(
    # This app-level token will be used only for establishing a connection
    app_token=os.environ.get("SLACK_ASK_ONE_APP_TOKEN"),  # xapp-A111-222-xyz
    # You will be using this WebClient for performing Web API calls in listeners
    web_client=WebClient(
        token=os.environ.get("SLACK_ASK_ONE_BOT_TOKEN")
    ),  # xoxb-111-222-xyz
)

from slack_sdk.socket_mode.response import SocketModeResponse
from slack_sdk.socket_mode.request import SocketModeRequest


def process(client: SocketModeClient, req: SocketModeRequest):
    if req.type == "events_api":
        # Acknowledge the request anyway
        response = SocketModeResponse(envelope_id=req.envelope_id)
        client.send_socket_mode_response(response)

        # Add a reaction to the message if it's a new message
        if (
            req.payload["event"]["type"] == "message"
            and req.payload["event"].get("subtype") is None
        ):
            client.web_client.reactions_add(
                name="eyes",
                channel=req.payload["event"]["channel"],
                timestamp=req.payload["event"]["ts"],
            )
    if req.type == "interactive" and req.payload.get("type") == "shortcut":
        if req.payload["callback_id"] == "hello-shortcut":
            # Acknowledge the request
            response = SocketModeResponse(envelope_id=req.envelope_id)
            client.send_socket_mode_response(response)
            # Open a welcome modal
            client.web_client.views_open(
                trigger_id=req.payload["trigger_id"],
                view={
                    "type": "modal",
                    "callback_id": "hello-modal",
                    "title": {"type": "plain_text", "text": "Greetings!"},
                    "submit": {"type": "plain_text", "text": "Good Bye"},
                    "blocks": [
                        {
                            "type": "section",
                            "text": {"type": "mrkdwn", "text": "Hello!"},
                        }
                    ],
                },
            )

    if req.type == "interactive" and req.payload.get("type") == "view_submission":
        if req.payload["view"]["callback_id"] == "hello-modal":
            # Acknowledge the request and close the modal
            response = SocketModeResponse(envelope_id=req.envelope_id)
            client.send_socket_mode_response(response)


# Add a new listener to receive messages from Slack
# You can add more listeners like this
client.socket_mode_request_listeners.append(process)
# Establish a WebSocket connection to the Socket Mode servers
print("connecting")
client.connect()
# Just not to stop this process
from threading import Event

Event().wait()
