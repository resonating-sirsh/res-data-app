"""GaphQL queries and wrappers for flow node runs."""

from datetime import datetime, timezone
from enum import Enum

from res.connectors.graphql import hasura
from res.utils.logging import logger
import res

client = hasura.Client()


class Status(Enum):
    """Statuses for a flow node run.."""

    NEW = "NEW"
    COMPLETED_SUCCESS = "COMPLETED_SUCCESS"
    COMPLETED_ERRORS = "COMPLETED_ERRORS"
    IN_PROGRESS = "IN_PROGRESS"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    ON_HOLD = "ON_HOLD"


class FlowNodes(Enum):
    """Flow Nodes in DxA."""

    EXPORT_STYLE_BUNDLE = "DXA_EXPORT_ASSET_BUNDLE"
    EXPORT_BODY_BUNDLE = "DXA_EXPORT_BODY_BUNDLE"
    BRAND_ONBOARDING = "BRAND_ONBOARDING"
    TEST = "TEST"


# TODO: In Hasura, we should add a pointer from `flow_node` -> `config`
# so that this lookup isn't necessary
flow_nodes_to_configs = {
    FlowNodes.BRAND_ONBOARDING.value: "onboarding_v0",
    FlowNodes.EXPORT_STYLE_BUNDLE.value: "techpack_v0",
    FlowNodes.EXPORT_BODY_BUNDLE.value: "body_bundle_v0",
    FlowNodes.TEST.value: "onboarding_v0",
}

WORKABLE_STATUSES = {Status.NEW.value, Status.ACKNOWLEDGED.value}
COMPLETED_STATUSES = {Status.COMPLETED_SUCCESS.value, Status.COMPLETED_ERRORS.value}


class Queries:
    get_all = """ query  {
         dxa_flow_node_run {
             id
             type
             created_at
             details
             status
             preset {
                 details
             }
             assets {
                 name path type status
             }
             updated_at
           }
         }"""
    with_assets = """
         query ($where: dxa_flow_node_run_bool_exp,
                $order_by: [dxa_flow_node_run_order_by!],
                $limit: Int) {
         dxa_flow_node_run(where: $where, order_by: $order_by, limit: $limit) {
             id
             type
             created_at
             details
             status
             preset {
                 details
             }
             assets {
                 name path type status
             }
             updated_at
           }
         }
        """


class Mutations:
    update = """
        mutation ($set: dxa_flow_node_run_set_input,
                  $where: dxa_flow_node_run_bool_exp!) {
          update_dxa_flow_node_run(_set: $set, where: $where) {
            affected_rows
            returning {
              id
              type
              status
              details
              preset {
                details
              }
            }
          }
        }
        """

    insert = """
        mutation ($type: dxa_flow_node_enum,
                  $preset_key: String, $details: jsonb, $priority: Int) {
          insert_dxa_flow_node_run(objects: {type: $type, preset_key: $preset_key,
                                        details: $details, priority: $priority}) {
            affected_rows
            returning {
              id
            }
          }
        }
    """


def metric(asset, verb, status):
    """
    - match: "flows.*.*.*.*.*"
      name: "flows"
      labels:
        flow: "$1"
        node: "$2"
        data_group: "$3"
        verb: "$4"
        status: "$5"
    """

    metric_name = f"flows.dxa.bertha.{asset}.{verb}.{status}"
    res.utils.logger.incr(metric_name, 1)


def get_all_jobs():
    """Fetch"""

    result = client.execute(Queries.get_all)
    return result["dxa_flow_node_run"]


async def fetch_by_id(run_id):
    """Fetch by id."""
    params = {"where": {"id": {"_eq": run_id}}}
    result = await client.execute_async(Queries.with_assets, params)
    flow_node_runs = result["dxa_flow_node_run"]
    if len(flow_node_runs) != 1:
        raise Exception(f"No run found for {run_id}")
    return flow_node_runs[0]


async def update_by_id(run_id, set_clause: dict):
    """Update by id."""
    params = {
        "set": set_clause,
        "where": {"id": {"_eq": run_id}},
    }
    resp = await client.execute_async(Mutations.update, params)
    return resp["update_dxa_flow_node_run"]["returning"]


def reset_job_status(run_id, status="NEW"):
    """Update by id."""
    params = {
        "set": {"status": status},
        "where": {"id": {"_eq": run_id}},
    }
    metric(run_id, "reset", status)
    return client.execute(Mutations.update, params)


async def update_status_by_id(run_id, status: str, details=None):
    """
    Update status by id.

    status must be one in `flow_node_run_status`
    """
    set_clause = {"status": status}
    if details:
        set_clause["status_details"] = details
    if status in COMPLETED_STATUSES:
        set_clause["ended_at"] = str(datetime.now(timezone.utc))

    resp = await update_by_id(run_id, set_clause)

    try:
        logger.incr("dxa.generate.style.bundle.jobs.completed", 1)
        logger.incr(
            "dxa.generate_style_bundle.jobs.%s" % status.lower().replace("_", "."), 1
        )
    except Exception as e:
        logger.warn("Couldn't update run metrics!", e)

    return resp


async def submit(run_type: str, details: dict, preset_key: str = None):
    """Submit a new run for a flow Node."""
    if preset_key is None:
        preset_key = flow_nodes_to_configs[run_type]

    params = {
        "type": run_type,
        "preset_key": preset_key,
        "details": details,
    }
    resp = await client.execute_async(Mutations.insert, params)
    metric(details.get("body_code", "any"), "submit", "OK")
    return resp["insert_dxa_flow_node_run"]["returning"][0]


def submit_synchronously(
    run_type: str, details: dict, preset_key: str = None, priority: int = 0
):
    """Submit a new run for a flow Node, but synchronously."""
    if preset_key is None:
        preset_key = flow_nodes_to_configs[run_type]

    params = {
        "type": run_type,
        "preset_key": preset_key,
        "details": details,
        "priority": priority,
    }

    resp = client.execute(Mutations.insert, params)

    metric(asset=details.get("body_code", "any"), verb=run_type, status="OK")

    return resp["insert_dxa_flow_node_run"]["returning"][0]


async def _fetch_oldest_workable(run_types: list = None):
    _and = [{"status": {"_in": list(WORKABLE_STATUSES)}}]
    if run_types is not None:
        _and.append({"type": {"_in": run_types}})

    # sorting by type desc will do bodies before styles, DXA_EXPORT .. BODY then ASSET
    params = {
        "where": {"_and": _and},
        "order_by": [
            {"type": "desc"},
            {"priority": "desc"},
            {"updated_at": "asc"},
        ],
        "limit": 1,
    }
    result = await client.execute_async(Queries.with_assets, params)
    return result["dxa_flow_node_run"]


async def claim_next(run_types=None):
    """
    Concurrency-safe method for a client to claim a run. Will return either 1 or 0 node runs.

    It would be great if we could just execute a query that said "take the next run"
    by throwing a row limit on the mutation that claims the requests,
    but I don't believe that's possible. Also, down the road we can use a grapql sub
    to remove the polling loop.
    """
    logger.info("Claiming next flow node run")
    next_claimable_request = await _fetch_oldest_workable(run_types)

    if not (len(next_claimable_request) > 0):
        logger.info("No runnables found")
        return []

    next_claimable_id = next_claimable_request[0]["id"]
    logger.info(f"Claiming run with id: {next_claimable_id}")

    # We probably want to "claim" node runs by setting some owner field.
    # Setting a status works for now, though.
    claimed = await client.execute_async(
        Mutations.update,
        {
            "set": {
                "status": "IN_PROGRESS",
                "started_at": str(datetime.now(timezone.utc)),
            },
            "where": {
                "_and": [
                    {"status": {"_in": list(WORKABLE_STATUSES)}},
                    {"id": {"_eq": next_claimable_id}},
                ]
            },
        },
    )

    logger.debug(f"Claimed request with id: {claimed}")

    if claimed["update_dxa_flow_node_run"]["affected_rows"] == 1:
        return claimed["update_dxa_flow_node_run"]["returning"]
    else:
        # This should be a very rare occurrence!
        logger.info(
            (
                f"Tried to claim run with id {next_claimable_id}, "
                "but it was taken by another runner. Trying to grab another."
            )
        )
        return await claim_next(run_types)
