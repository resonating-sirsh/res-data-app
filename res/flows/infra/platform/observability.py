import res
from res.flows import FlowContext
from res.connectors.slack.SlackConnector import ingest_slack_channels
from res.flows.make.healing.piece_healing_monitor import (
    HealingAlertDataProcessor,
    HealingAlertSlackPoster,
)
import res


@res.flows.flow_node_attributes(memory="10Gi")
def send_healing_status_update(event, context={}):
    """
    Take a look at lagging assets and post some alerts to slack
    """
    alertData = HealingAlertDataProcessor()
    slackPoster = HealingAlertSlackPoster(alertData, slack_channel="one-platform-x")
    # post a bunch of alerts to slack
    slackPoster.post_healings_to_slack()
    slackPoster.post_lagging_non_healings_to_slack()
    slackPoster.post_recently_printed_healings()
    slackPoster.post_make_node_finished_assets_todo()
    slackPoster.post_old_print_assets()
    # and write to a kafka topic
    slackPoster.post_long_laggers_to_kafka_alerts()
    return {}


def slack_ingestion_handler(event, context={}):
    with FlowContext(event, context) as fc:
        since_date = fc.args.get("since_date")

        ingest_slack_channels(date=since_date)

    return {}


def coda_ingestion_handler(event, context={}):
    with FlowContext(event, context) as fc:
        since_date = fc.args.get("since_date")

    return {}


def shortcut_ingestion_handler(event, context={}):
    with FlowContext(event, context) as fc:
        since_date = fc.args.get("since_date")

    return {}


@res.flows.flow_node_attributes(memory="80Gi")
def handler(event, context={}):
    slack_ingestion_handler(event, context)
    coda_ingestion_handler(event, context)
    shortcut_ingestion_handler(event, context)


def invoke_handler(env="development"):
    event = {
        "apiVersion": "resmagic.io/v1",
        "kind": "resFlow",
        "metadata": {
            "name": "infra.platform.observability.handler",
            "version": "whatevs",
        },
        "assets": [],
        "task": {"key": "submitted-from-res"},
        "args": {},
    }

    template_name = "res-flow-node"
    url = f"https://data.resmagic.io/res-connect/flows/{template_name}"
    if env == "development":
        url = f"https://datadev.resmagic.io/res-connect/flows/{template_name}"

    return res.utils.safe_http.request_post(url, json=event)


def the_slack_news(
    event={},
    context={},
    days_back=2,
    notification_channel="artificial-news",
    plan=False,
):
    """
    Use an LLM to search for recent news that we indexed in our vector stores
    """
    from res.observability.io import list_stores, open_store
    from res.learn.agents import InterpreterAgent
    from res.learn.agents.InterpreterAgent import DEFAULT_MODEL
    import time

    agent = InterpreterAgent()
    slack = res.connectors.load("slack")

    date = res.utils.dates.utc_days_ago(days_back).isoformat()
    res.utils.logger.info(f"news since {date}")
    channels = [
        {"node": "ONE", "channels": ["are_we_flowing", "contractvariables"]},
        {
            "node": "Make & Sew",
            "channels": [
                "sewing-sets",
                "set_processing",
                "material-coverage",
                "assemble_node",
                "make_one_team",
                "",
            ],
        },
        {
            "node": "Create-ONE & Product",
            "channels": [
                "product_development_2023",
                "create-one",
                "trims",
                "thekit-brandsuccess",
            ],
        },
        {
            "node": "DxA",
            "channels": ["sewing-sets", "material-coverage", "assemble_node"],
        },
    ]

    PROMPT = """Please describe with specific details an overview of the provided slack messages since yesterday and comment on any concerns or successes that were discussed. 
    Use enumerations, bullet points and any provided links to attachments that could help the reader learn more. 
    Slack links take the format <{URL}|TEXT>."""

    summary_all = ""
    for record in channels:
        data = ""
        for channel in record["channels"]:
            name = channel
            store = open_store(
                **{"name": channel, "namespace": "slack", "type": "vector-store"}
            )
            res.utils.logger.info(f"{store._entity} summary")
            summary = store.query(f"SELECT * FROM dataset where timestamp > '{date}'")[
                ["text", "timestamp"]
            ].to_dict("records")
            if not summary:
                res.utils.logger.info(f"Nothing happened lately...")
                continue
            data += f"\n## Channel: {name}\n{summary}"
        if data:
            res.utils.logger.info(
                f"building a summary of text of size {len(data)} in {record}"
            )
            response = (
                "\n\n"
                + agent.summarize(PROMPT, data, model=DEFAULT_MODEL)[
                    "summarized_response"
                ]
            )
            summary_all += response

    res.utils.logger.info(
        f"building an overall summary of text of size {len(summary_all)}"
    )

    response = agent.summarize(
        "With this understanding that this is a summary of slack discussions within our company 'Resonance' which designs, sells and makes garments, please provide an overview summary of the following text to answer the question of 'what was discussed in general' in two or three sentences without any specific details of what was described. What are the overall concerns and successes",
        summary_all,
        model=DEFAULT_MODEL,
    )["summarized_response"]

    if len(response) and not plan:
        res.utils.logger.info(f"slacking overview to {notification_channel}")
        slack(
            {
                "slack_channels": [notification_channel],
                "message": f"# `OVERVIEW` \n {response}",
            }
        )
    if len(response) and not plan:
        res.utils.logger.info(f"slacking details...")
        slack(
            {
                "slack_channels": [notification_channel],
                "message": f"# `SUMMARY` \n {summary_all}",
            }
        )
    elif not plan:
        slack(
            {
                "slack_channels": [notification_channel],
                "message": f"no news today - but we did check - is no news good news?",
            }
        )
    return summary_all


def send_validate_acq_inputs_message(agent, store):
    res.utils.logger.info("building acq message: validate inputs")
    data = store.query(
        "select name, node, contracts_failing_list, entered_at, last_updated_at, owner, airtable_link from Style where node = 'Validate Apply Color Request Inputs'"
    ).to_dicts()

    # remove the TTs like this
    data = [d for d in data if "TT-" not in d["name"]]

    s = agent.ask(
        f"You are creating a report about styles in `Validate Apply Color Request Inputs` stage of the apply color queue. Using enumerations and bulleted lists, please provide an elaborate and detailed summary of the styles listed by name by observing things that have not been updated in a few days or any failing contracts if relevant. Provide airtable links in your report and stress which ones have not been updated for the longest time and require attention: {data}"
    )
    slack = res.connectors.load("slack")
    slack({"slack_channels": ["dxa-status-updates"], "message": s})
    res.utils.logger.info("done")


def send_meta_one_stuck_message(agent, store):
    res.utils.logger.info(
        "building acq message: generate meta one and export color pieces"
    )
    data = store.query(
        "select name, node, contracts_failing_list, entered_at, last_updated_at, owner, airtable_link from Style where node = 'Generate Meta ONE' OR node = 'Export Colored Pieces' or node = 'DxA Exit' "
    ).to_dicts()

    # remove the TTs like this
    data = [d for d in data if "TT-" not in d["name"]]

    s = agent.ask(
        f"You are creating a report about styles in `Generate Meta ONE, Exporting Asset` or `DxA Exit` stages of the apply color queue. Using enumerations and bulleted lists, please provide an elaborate and detailed summary of the styles listed by name by observing things that have not been updated in a few days or any failing contracts if relevant. Provide airtable links in your report and stress which ones have not been updated for the longest time and require attention: {data}"
    )
    slack = res.connectors.load("slack")
    slack({"slack_channels": ["dxa-status-updates"], "message": s})
    res.utils.logger.info("done")


def send_dxa_status_messages():
    from res.observability.queues import Style
    from res.observability.io import ColumnarDataStore
    from res.learn.agents import InterpreterAgent

    agent = InterpreterAgent()
    style_store = ColumnarDataStore(
        Style,
        description="A store for searching for state of styles in the apply color queue",
    )

    send_meta_one_stuck_message(agent, style_store)
    send_dxa_status_messages(agent, style_store)
