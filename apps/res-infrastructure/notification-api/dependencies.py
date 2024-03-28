from typing import List

from handlers import NotificationHandler
from handlers.CreateONENotifcationHandler import CreateONENotificationHanderl
from handlers.SlackNotificationHandler import SlackNotificationHandler
from handlers.EmailNotificationHandler import EmailNotificationHandler
from controllers.email import SendgridEmailController
from res.connectors.airtable.AirtableClient import ResAirtableClient
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.connectors.graphql.hasura import Client as HasuraClient
from res.connectors.slack.SlackConnector import SlackConnector


def get_handlers():
    graphql_client = ResGraphQLClient()
    hasura_client = HasuraClient()
    slack_connector = SlackConnector()
    handlers: List[NotificationHandler] = [
        CreateONENotificationHanderl(hasura_client),
        SlackNotificationHandler(graphql_client, slack_connector=slack_connector),
        EmailNotificationHandler(
            controller=SendgridEmailController(),
        ),
    ]

    yield handlers


def get_airtable_client():
    airtable_client = ResAirtableClient()
    yield airtable_client
