from res.connectors.airtable.AirtableWebhookSession import AirtableWebhookSession
from _pytest.compat import ascii_escaped
import pytest
import res
import json
from ..main import app, PROCESS_NAME

app.testing = True


@pytest.mark.service
def test_receives_payload():
    """
    test post to f"/{PROCESS_NAME}/<profile>/<base>/<table>"
    {
        "base": {"id": "appXXX"},
        "webhook": {"id": "achYYY"},
        "timestamp": "2020-03-11T21:25:05.663Z"
    }

    Call the endpoint without suppression after adding a test change to a test table....

    """
    from res.connectors.airtable import AirtableWebhooks

    with app.test_client() as client:

        base_id = "appaaqq2FgooqpAnQ"
        table_id = "tblt9jo5pmdo0htu3"
        wid = None  # could resolve a valid id but may not matter for this test

        connector = res.connectors.load("airtable")[base_id][table_id]
        # this is just a support tool to get the current cursor so we can prove that we can get newer changes
        cursor = AirtableWebhooks.get_current_cursor(base_id, table_id)

        # update a sell on a sample row
        rec_example = dict(connector.to_dataframe().iloc[0])
        rec_example[
            "IssueReason"
        ] = f"Updated reason on {res.utils.dates.utc_now_iso_string()}"
        connector.update_record(rec_example)

        event = {
            "base": {"id": base_id},
            "webhook": {"id": wid},
            "timestamp": "2020-03-11T21:25:05.663Z",
        }

        # for testing we do not suppress errors so we capture the error flow
        result = client.post(
            f"{PROCESS_NAME}/default/{base_id}/{table_id}?suppress_errors=false",
            data=json.dumps(event),
        )

        assert result.status_code == 200
