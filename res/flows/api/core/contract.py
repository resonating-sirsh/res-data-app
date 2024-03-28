import res
from schemas.pydantic.flow import User, Node, Contract
from res.flows.api.core.user import ensure_users_exist
from res.flows.api.core.node import ensure_nodes_exist


def add_contract(contract_typed, hasura=None):
    hasura = hasura or res.connectors.load("hasura")
    owner = contract_typed.owner
    nodes = [contract_typed.node] + contract_typed.required_exiting_flows
    nodes = [Node(name=n) for n in nodes]
    owner = User(email=owner)
    ensure_users_exist([owner], hasura=hasura)
    ensure_nodes_exist(nodes, hasura=hasura)

    return add_contracts([contract_typed], hasura=hasura)


def add_contracts(contracts_typed, hasura=None):

    hasura = hasura or res.connectors.load("hasura")
    contracts = [c.db_dict() for c in contracts_typed]

    # TODO this will read from the api/upserts instead of coded here
    UPSERT_MANY = """mutation upsert_contracts($contracts: [flow_contracts_insert_input!] = {}) {
        insert_flow_contracts(objects: $contracts, on_conflict: {constraint: contracts_pkey, update_columns: [key, name, node_id, owner_id, status, metadata, required_exiting_flows ]}) {
            returning {
                key
                id
                status
                description
                node {
                    id
                    name
                }
                user {
                    email
                }
                metadata
            }
          }
        }
        """

    return hasura.execute_with_kwargs(UPSERT_MANY, contracts=contracts)


def _bootstrap_contracts(hasura=None, plan=False):

    contracts = [
        {
            "key": "BODY_ONE_READY",
            "owner": "sirsh@resonance.nyc",
            "description": "The final contracts checked for body one ready flow",
            "node": "ONE.Meta.Body",
            "metadata": {"auto_validated": False},
            "required_exiting_flows": ["ONE.Meta.Body"],
        },
    ]

    contracts = [Contract(**n) for n in contracts]

    if plan:
        return contracts

    return add_contracts(contracts_typed=contracts, hasura=hasura)
