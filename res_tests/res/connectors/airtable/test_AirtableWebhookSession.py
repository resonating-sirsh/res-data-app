from res import connectors
from res.connectors.airtable import (
    AirtableConnector,
    AirtableWebhookSession,
    AirtableWebhookSpec,
    AirtableWebhooks,
)
import res
import pytest
import os


@pytest.mark.service
def test_workflow():
    # these are the defaults anyway but just being explicit
    os.environ["RES_NAMESPACE"] = "testing"
    os.environ["RES_APP_NAME"] = "testing"

    base_id = "appaaqq2FgooqpAnQ"
    table_id = "tblt9jo5pmdo0htu3"
    # wid = ""

    connector = res.connectors.load("airtable")[base_id][table_id]
    # this is just a support tool to get the current cursor so we can prove that we can get newer changes
    cursor = AirtableWebhooks.get_current_cursor(base_id, table_id)

    # update a sell on a sample row
    rec_example = dict(connector.to_dataframe().iloc[0])
    rec_example[
        "IssueReason"
    ] = f"Updated reason on {res.utils.dates.utc_now_iso_string()}"
    connector.update_record(rec_example)

    # check that when we consume we get some changes for this table
    with AirtableWebhookSession(base_id, table_id) as session1:
        data = session1.changes
        assert len(data) > 0
        cursor = session1.cursor

    # check that when we recreate the session we still have our cursor
    # this uses the session restore flow
    with AirtableWebhookSession(base_id, table_id) as session2:
        assert session2.cursor == cursor, "The cursor was not correctly restored"
        webhook_id = session2.webhook_id
        cursor = session1.cursor

    with AirtableWebhookSession(base_id, table_id, webhook_id=webhook_id) as session2:
        assert session2.cursor == cursor, "The cursor was not correctly restored"
        assert (
            len(session2.changes) == 0
        ), "After flushing the cursor and re-reading there should not be changes on the test table (although it could have been updated in the interim)"
