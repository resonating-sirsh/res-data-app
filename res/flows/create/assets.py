"""GaphQL queries and wrappers for `create.assets`."""


from res.connectors.graphql import hasura

client = hasura.Client()


class Queries:
    """Queries for create.assets."""

    query = """
        query ($where:create_asset_bool_exp) {
          create_asset(where:$where) {

            id created_at details is_public_facing
            name path status style_id type

            job {
              details type status status_details id
            }
          }
        }
    """


class Mutations:
    """Mutations for create.assets."""

    insert_many = """
        mutation ($objects: [create_asset_insert_input!]!) {
          insert_create_asset(objects: $objects) {
            returning {
              id
            }
          }
        }
        """

    update = """
        mutation(
          $set: create_asset_set_input,
          $where: create_asset_bool_exp!
        ) {
          update_create_asset(_set: $set, where: $where) {
            returning {
              id
            }
          }
        }
        """


async def fetch_job_assets(job_id: str):
    """Fetch all create.assets for a platform.job."""
    # client = hasura.Client()
    params = {"job_id": job_id}
    results = await client.execute_async(Queries.query, params)
    return results["create_asset"]


async def update_by_id(asset_id: str, set_clause: dict) -> dict:
    """
    Update an asset for a given id.

    Returns dict containing id of updated record
    """
    # client = hasura.Client()
    params = {
        "set": set_clause,
        "where": {"id": {"_eq": asset_id}},
    }
    results = await client.execute_async(Mutations.update, params)
    return results["update_create_asset"]["returning"]


async def insert(asset) -> dict:
    """
    Insert a create.asset.

    Returns dict containing id of inserted record
    """
    # client = hasura.Client()
    params = {"objects": [asset]}
    results = await client.execute_async(Mutations.insert_many, params)
    return results["insert_create_asset"]["returning"]


async def hide_style_assets_for_style(style_id) -> list:
    """
    Hides assets from Brand Onboarding users.

    For brand onboarding, we currently limit permissions to assets. So the
    brand onboarding user cannot access records where `is_public_facing` is
    False

    returns updated records
    """
    # client = hasura.Client()
    params = {
        "where": {"style_id": {"_eq": style_id}},
        "set": {"is_public_facing": False},
    }
    results = await client.execute_async(Mutations.update, params)
    return results["update_create_asset"]["returning"]
