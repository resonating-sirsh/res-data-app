import res
from schemas.pydantic.flow import User


def ensure_users_exist(users_typed, hasura=None):

    return add_users(users_typed, check_exists=True, hasura=hasura)


def add_users(users_typed, check_exists=False, hasura=None):
    if not isinstance(users_typed, list):
        users_typed = [users_typed]

    users = [u.db_dict() for u in users_typed]

    update_list = "email,alias" if check_exists else "email,alias,slack_uid"

    hasura = hasura or res.connectors.load("hasura")
    UPSERT_MANY = f"""
          mutation upsert_users($users: [flow_users_insert_input!] = {{}}) {{
              insert_flow_users(objects: $users, on_conflict: {{constraint: users_pkey, update_columns: [{update_list}]}}) {{
                returning {{
                  id
                  email
                }}
              }}
            
          }}

    """

    return hasura.execute_with_kwargs(UPSERT_MANY, users=users)


def _bootstrap_users(hasura=None, data=None, plan=False):
    """
    seed the database with nodes
    """

    users = [
        {"email": "sirsh@resonance.nyc", "alias": "sirsh", "slack_uid": "U01JDKSB196"},
        {"email": "lbluitt@resonance.nyc", "alias": "lorelay", "slack_uid": ""},
        {"email": "jleonard@resonance.nyc", "alias": "john", "slack_uid": ""},
        {
            "email": "sew-development@resonance.nyc",
            "alias": "sew-development",
            "slack_uid": "",
        },
    ]

    # if data is not None:
    #     # pass in data matching schema
    #     users += data[["email", "alias", "slack_uid"]].to_dict("records")

    users = [User(**n) for n in users]
    if plan:
        return users

    return add_users(users, hasura=hasura)
