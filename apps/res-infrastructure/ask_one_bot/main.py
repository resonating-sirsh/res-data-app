#####
# # https://python.plainenglish.io/lets-create-a-slackbot-cause-why-not-2972474bf5c1
# # https://slack.dev/python-slack-sdk/web/index.html
# # https://medium.com/developer-student-clubs-tiet/how-to-build-your-first-slack-bot-in-2020-with-python-flask-using-the-slack-events-api-4b20ae7b4f86
# # https://slack.dev/bolt-python/tutorial/getting-started-http
# # https://github.com/slackapi/bolt-python/issues/185
# # https://blog.tryfrindle.com/deploying-a-slack_bolt-app-with-gunicorn/
# # https://github.com/slackapi/bolt-python/blob/main/examples/socket_mode_healthcheck.py
# # https://slack.dev/bolt-python/tutorial/getting-started-http
# # https://github.com/slackapi/bolt-python/issues/185
# # https://blog.tryfrindle.com/deploying-a-slack_bolt-app-with-gunicorn/
### ## nudge 24+
#####

import os
import re
from flask import Flask, make_response
from flask import Flask, Response, make_response
from slack_sdk import WebClient
from slack_bolt import App, Say
from slack_bolt.adapter.flask import SlackRequestHandler
import pandas as pd
import res
from res.utils.secrets import secrets
from res.flows.meta.ONE.instantiate.agent import BodyIntakeAgent

import traceback
from res.learn.agents import InterpreterAgent
from res.observability.queues import try_get_queue_agent

# if we are in the right context this works....
secrets.get_secret("SLACK_ASK_ONE_APP_TOKEN")
secrets.get_secret("SLACK_ASK_ONE_SIGNING_SECRET")
secrets.get_secret("SLACK_ASK_ONE_BOT_TOKEN")
secrets.get_secret("RES_META_ONE_API_KEY")

os.environ["OPENAI_API_KEY"] = secrets.get_secret("ASK_ONE_OPENAI_API_KEY")


from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from res.learn.agents.StaticMetaOneAgent import StaticMetaOneAgent

#
# Socket Mode Bolt app
#

# Install the Slack app and get xoxb- token in advance
app = App(token=os.environ["SLACK_ASK_ONE_BOT_TOKEN"])
socket_mode_handler = SocketModeHandler(app, os.environ["SLACK_ASK_ONE_APP_TOKEN"])

my_agent = None
my_queue_agent = None
static_meta_one_agent = None
body_agent = None

try:
    # switch to this one
    static_meta_one_agent = BodyIntakeAgent()  # StaticMetaOneAgent()
    res.utils.logger.info(f"Loaded static meta one agent")
except:
    res.utils.logger.warn(f"Failed {traceback.format_exc()}")
try:
    res.utils.logger.info(f"Loading the super agent")
    # can maybe add hints on the different routes below later
    my_agent = InterpreterAgent()
    res.utils.logger.info(f"Loading the queue agent")
    # load the queue agent which is a more focused individual about status
    my_queue_agent = try_get_queue_agent()
    res.utils.logger.info(f"Loaded agents")

except:
    res.utils.logger.warn(f"Failed {traceback.format_exc()}")


@app.event("app_mention")
def event_test(event, say):
    """
    https://api.slack.com/events/app_mention
    """

    username = f"<@{event['user']}>"
    channel_name = event.get("channel")
    reply_ts = event.get("thread_ts", event.get("ts"))

    say(f"Hi there, <@{event['user']}>! I'm thinking...", thread_ts=reply_ts)

    message = event["text"]
    res.utils.logger.info(
        f"APPMEN-HANDLER: {message} - {channel_name} - {username} - {reply_ts=}"
    )
    try:
        active_agent = my_agent
        if channel_name in ["C01Q05P44K0", "one-platform-status"]:
            res.utils.logger.info(f"For channel {channel_name} - using queue agent")
            active_agent = my_queue_agent
        if channel_name in ["C06JQSKAF6Y", "C0690NULKLY"]:
            active_agent = static_meta_one_agent

        response = active_agent(
            message,
            user_context=username,
            channel_context=channel_name,
            response_callback=say,
            thread_ts=reply_ts,
        )
        # in this mode we dont have a callback (yet)
        # if channel_name in ["C06JQSKAF6Y", "C0690NULKLY"]:
        #    say(response)

        # say(response)
    except Exception as ex:
        res.utils.logger.warn(f"{traceback.format_exc()}")
        say(
            f"Sorry, I could not figure that out <@{event['user']}>", thread_ts=reply_ts
        )


@app.event("help")
def handle_message_events(message, say):
    text = message["text"]
    if text.lower() in ["help", "help me"]:
        say(
            "Check out the <{https://coda.io/d/Technology_dZNX5Sf3x2R/ASK-ONE_su5Vx#_luGng}|{chat guide}"
        )


@app.message(".*")
def generic_message_handler(event, say, logger):
    username = f"<@{event['user']}>"
    channel_name = event.get("channel")
    reply_ts = event.get("thread_ts", event.get("ts"))
    say(f"Hi there, <@{event['user']}>! I'm thinking...", thread_ts=reply_ts)
    # use slack lookup to resolve user id and channel later...
    # could get a thread id to and then we could reply to the user in streaming fashion?? or we could just stream out responses in a callback
    message = event["text"]
    res.utils.logger.info(
        f"GEN-HANDLER: {message} - {channel_name} - {username} - {reply_ts=}"
    )

    try:
        active_agent = my_agent
        if channel_name in ["C01Q05P44K0", "one-platform-status"]:
            logger.info(f"For channel {channel_name} - using queue agent")
            active_agent = my_queue_agent
        if channel_name in ["C06JQSKAF6Y", "C0690NULKLY"]:
            active_agent = static_meta_one_agent

        response = active_agent(
            message,
            user_context=username,
            channel_context=channel_name,
            response_callback=say,
            thread_ts=reply_ts,
        )
        # say(response)
    except Exception as ex:
        res.utils.logger.warn(f"{traceback.format_exc()}")
        say(
            f"Sorry, I could not figure that out <@{event['user']}>", thread_ts=reply_ts
        )


@app.command("/one")
def one_h(ack, say, command):
    ack()

    user_id = command["user_id"]
    agent = my_agent()
    say(f"<@{user_id}>, im looking up the ONE, give me a moment to do the thinking... ")
    response = agent(command["text"])
    say(response)


@app.command("/fulfillment")
def fulf_h(ack, say, command):
    ack()

    user_id = command["user_id"]
    agent = my_agent
    say(
        f"<@{user_id}>, im looking up the orders, give me a moment to do the thinking... "
    )
    response = agent(command["text"])
    say(response)


@app.command("/bodies")
def bodies_h(ack, say, command):
    ack()

    user_id = command["user_id"]
    agent = my_agent
    say(
        f"<@{user_id}>, im looking up the bodies, give me a moment to do the thinking... "
    )
    response = agent(command["text"])
    say(response)


@app.command("/styles")
def styles_h(ack, say, command):
    ack()

    user_id = command["user_id"]
    agent = my_agent
    say(
        f"<@{user_id}>, im looking up the styles, give me a moment to do the thinking... "
    )
    response = agent(command["text"])
    say(response)


flask_app = Flask(__name__)


@flask_app.route("/health", methods=["GET"])
def slack_events():
    if (
        socket_mode_handler.client is not None
        and socket_mode_handler.client.is_connected()
    ):
        return make_response("OK", 200)
    return make_response("The Socket Mode client is inactive", 503)


if __name__ == "__main__":
    socket_mode_handler.connect()  # does not block the current thread.
    flask_app.run(port=5000)
