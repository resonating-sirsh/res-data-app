import res.flows.meta.one_marker.apply_color_request as apply_color_request
import res.flows.meta.one_marker.body as body
from res.flows.dxa import node_run
from res.flows.dxa import event_handler as dxa_event_handler
from res.flows.create import assets
from res.connectors.graphql import hasura

import asyncio
import os
import pytest
from unittest.mock import MagicMock

if "resmagic" in os.getenv("HASURA_ENDPOINT"):
    # We need to figure out a more elegant way to do this.
    raise Exception("Uh oh. Looks like you're running this against a nonlocal Hasura.")


# Leaving this mutation in the tests, as we should _never_ delete from prod
DELETE_ALL_DXA_MUTATION = """
mutation MyMutation {
  delete_create_asset(where:{id:{_nin:[]}}) {
    returning {
      id
    }
  }
  delete_dxa_flow_node_run(where:{id:{_nin:[]}}) {
    returning {
      id
    }
  }
}
"""

_client = hasura.Client()


@pytest.fixture
def _cleanup():
    yield
    _client.execute(DELETE_ALL_DXA_MUTATION)


# TODO: pip install for pytest-asyncio
@pytest.mark.usefixtures("_cleanup")
@pytest.mark.asyncio
async def test_flow_node_run_loop():
    """
    Tests inserting a few node_runs, claiming them concurrently,
    and creating an asset for them.
    """
    new_node_runs = await asyncio.gather(
        node_run.submit(node_run.FlowNodes.TEST.value, {}),
        node_run.submit(node_run.FlowNodes.TEST.value, {}),
    )

    # Tests that runners can claim node_runs concurrently
    claimed = [
        run[0]
        for run in (
            await asyncio.gather(
                node_run.claim_next(node_run.FlowNodes.TEST.value),
                node_run.claim_next(node_run.FlowNodes.TEST.value),
            )
        )
    ]

    unique_claimed_ids = {c.get("id") for c in claimed}
    assert len(unique_claimed_ids) == 2

    claimed_statuses = {c.get("status") for c in claimed}
    assert claimed_statuses == {node_run.Status.IN_PROGRESS.value}

    # Add some assets
    await asyncio.gather(
        *[
            assets.insert(
                {
                    "path": "s3://wherever",
                    "job_id": claimed_id,
                    "type": "MEDIA_RENDER",
                    "style_id": "s_123",
                    "body_id": "b_123",
                    "name": "my_asset",
                    "is_public_facing": False,
                    "status": "UPLOADED",
                }
            )
            for claimed_id in unique_claimed_ids
        ]
    )


def _mocked_styles_query_resp(style_id: str, body_code: str):
    return {
        "data": {
            "style": {
                "id": style_id,
                "body": {"code": body_code},
                "artworkFile": {
                    "file": {"s3": {"bucket": "res-magic", "key": "blah/blah.jpg"}}
                },
            }
        }
    }


@pytest.mark.asyncio
@pytest.mark.usefixtures("_cleanup")
async def test_brand_onboarding_event():
    """
    Test that the brand onboarding processor creates a valid job.
    """
    onboarding_event = {
        "event_type": "CREATE",
        "job_type": node_run.FlowNodes.BRAND_ONBOARDING.value,
    }
    style_id = "rec_style_123"
    body_code = "rec_body_123"
    dxa_event_handler.ResGraphQLClient.query = MagicMock(
        return_value=_mocked_styles_query_resp(style_id, body_code)
    )

    # This is what we're actually testing
    dxa_event_handler.handle(onboarding_event)

    runs = await node_run.claim_next(
        run_types=[node_run.FlowNodes.BRAND_ONBOARDING.value]
    )

    assert len(runs) == 1
    run = runs[0]

    details = run["details"]
    assert style_id == details.get("style_id")
    assert body_code == details.get("body_code")


@pytest.mark.asyncio
@pytest.mark.usefixtures("_cleanup")
async def test_export_style_bundle_update():
    """Test processing an export asset bundle `update` event."""
    apply_color_request.update_apply_color_job_status = MagicMock()
    requestor_id = "req_123"
    style_event = {
        "event_type": hasura.ChangeDataTypes.UPDATE.value,
        "status": node_run.Status.IN_PROGRESS,
        "requestor_id": requestor_id,
        "job_type": node_run.FlowNodes.EXPORT_STYLE_BUNDLE.value,
    }

    dxa_event_handler.handle(style_event)

    next_run = await node_run.claim_next()

    assert len(next_run) == 0, "No node_runs should have been submitted"


@pytest.mark.asyncio
@pytest.mark.usefixtures("_cleanup")
async def test_export_style_bundle_create():
    """Test processing an export asset bundle `create` event."""
    apply_color_request.get_export_asset_bundle_job_inputs = MagicMock(return_value={})

    requestor_id = "req_123"
    style_event = {
        "event_type": hasura.ChangeDataTypes.CREATE.value,
        "status": node_run.Status.IN_PROGRESS,
        "requestor_id": requestor_id,
        "job_type": node_run.FlowNodes.EXPORT_STYLE_BUNDLE.value,
    }

    dxa_event_handler.handle(style_event)

    next_run = await node_run.claim_next()

    assert len(next_run) == 1, "A node_run should have been submitted"


@pytest.mark.asyncio
@pytest.mark.usefixtures("_cleanup")
async def test_export_body_bundle_create():
    """Test processing a body asset bundle `create` event."""
    body.get_dxa_export_body_bundle_details = MagicMock(return_value={})

    requestor_id = "req_123"
    event = {
        "event_type": hasura.ChangeDataTypes.CREATE.value,
        "status": node_run.Status.IN_PROGRESS,
        "requestor_id": requestor_id,
        "job_type": node_run.FlowNodes.EXPORT_BODY_BUNDLE.value,
    }

    dxa_event_handler.handle(event)

    next_runs = await node_run.claim_next()
    assert len(next_runs) == 1, "A node_run should have been submitted"

    next_run = next_runs[0]
    assert (
        next_run.get("type") == node_run.FlowNodes.EXPORT_BODY_BUNDLE.value
    ), "The created run should be for body bundles"
