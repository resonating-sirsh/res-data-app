import res
import pytest
import os


@pytest.mark.service
def test_workflow():
    from res.connectors.airtable import (
        AirtableWebhookSpec,
        AirtableWebhooks,
        AirtableConnector,
    )

    # these are the defaults anyway but just being explicit
    os.environ["RES_NAMESPACE"] = "testing"
    os.environ["RES_APP_NAME"] = "testing"

    field_names = ["Name", "IssueReason", "IssueTags", "modified"]
    base_id = "appaaqq2FgooqpAnQ"
    table_id = "tblt9jo5pmdo0htu3"
    spec = AirtableWebhookSpec.get_spec_for_field_names(base_id, table_id, field_names)

    assert (
        spec._base_id == base_id
    ), "the base id should be set as a property of the spec"
    assert (
        spec.table_id == table_id
    ), "the table id should be set as a property of the spec"

    # test save to dynamo
    spec.save_config()

    # should be able to reload it
    loaded_spec = AirtableWebhookSpec.load_config(base_id, table_id)

    assert loaded_spec is not None, "Failed to load a spec for this base/table"
    assert loaded_spec == spec, "The loaded spec does not equal the one we saved"

    wid, remote_spec = spec.push()

    assert (
        wid is not None
    ), "When we pushed a webhook we did not get back a valid webhook id"
    assert (
        spec == remote_spec
    ), "The pushed spec does not equal the remote as it should in a trivial way"

    wid, remote_spec = spec.pull()

    assert spec == remote_spec, "The remote spec does not equal the pushed spec"

    cursor = AirtableWebhooks.get_current_cursor(base_id, table_id)

    # TODO what test on the cursor?

    webhooks = AirtableWebhooks.get_base_webhooks(base_id=base_id, table_id=table_id)

    assert (
        len(webhooks) == 1
    ), "There should be one and only one webhook for this base/table"


def test_dont_change_webhook_for_same_spec():
    pass


def test_dont_create_multiple_webhooks():
    pass


@pytest.mark.service
def test_load_all():
    from res.connectors.airtable import AirtableWebhookSpec

    data = AirtableWebhookSpec.load_all()
    assert len(data) > 0, "Did not load any specs"
