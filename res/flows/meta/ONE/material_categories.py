import res.connectors
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils.dataframes import remap_df_to_dicts
from res.connectors.postgres.PostgresConnector import PostgresConnector
from res.connectors.postgres.PostgresDictSync import PostgresDictSync
from res.utils.logging import logger

import datetime


def parse_timestamp(timestamp):
    split = timestamp.split(":")
    split.pop()
    return datetime.datetime.fromisoformat(":".join(split))


def fetch_material_categories_from_airtable():
    ac = res.connectors.load("airtable")
    pop_column = lambda x: x[0] if isinstance(x, list) else x
    null_column = lambda x: None if isinstance(x, float) else x
    material_category_table = ac.get_table("appa7Sw0ML47cA8D1/tblTppSkM6TpAnbOW")
    material_category_df = material_category_table.to_dataframe()
    material_category_df = material_category_df[
        [
            "Name",
            "Code",
            "Use",
            "Materials",
            "record_id",
            "Sewing Cells",
            "Created At",
            "Updated At",
        ]
    ]

    material_categories = remap_df_to_dicts(
        material_category_df,
        {
            "Name": "name",
            "Code": "code",
            "Use": {"use": pop_column},
            "Materials": {"materials": null_column},
            "record_id": "metadata__airtable_record_id",
            "Sewing Cells": "metadata__sewing_cells",
            "Created At": "metadata__airtable_created_at",
            "Updated At": "metadata__airtable_updated_at",
        },
    )

    return material_categories


qgl_material_categories_by_code = """
query materials_category_by_code($code: String!) {
  meta_material_categories(where: {code: {_eq: $code}}) {
    code
    created_at
    id
    materials
    metadata
    name
    updated_at
    use
  }
}
"""


def fetch_material_categories_from_hasura(code: str) -> dict:
    gql = ResGraphQLClient()
    logger.info(f"fetch_material_categories_from_hasura code={code}")
    result = gql.query(qgl_material_categories_by_code, variables={"code": code},)[
        "data"
    ]["meta_material_categories"]
    logger.info(f"fetch_material_categories_from_hasura result={result}")
    if len(result) == 0:
        return None
    return result[0]


def run_sync():
    records = fetch_material_categories_from_airtable()
    conn = PostgresConnector()
    sync = PostgresDictSync(
        "meta.material_categories",
        records,
        on={"metadata": "airtable_record_id"},
        pg=conn,
        ignore_columns_for_update=["id", "created_at", "updated_at"],
    )
    sync.sync()
