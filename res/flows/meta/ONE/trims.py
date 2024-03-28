from collections.abc import Iterable
import hashlib
import math
import pandas as pd

import res.connectors
from res.connectors.graphql import ResGraphQLClient
from res.connectors.postgres.PostgresConnector import PostgresConnector
from res.connectors.postgres.PostgresDictSync import PostgresDictSync

from res.utils.logging import logger


def pop_column(x):
    if isinstance(x, list):
        if len(x) == 0:
            return None
        else:
            return x[0]
    return x


null_column = lambda x: None if isinstance(x, float) and math.isnan(x) else x
join_column = lambda x: ", ".join(x) if isinstance(x, Iterable) else x
bool_column = lambda x: True if x is True else False
identity_column = lambda x: x

gql_trim_by_body_code = """
query MetaTrims($body_code: String!, $version: numeric!) {
  meta_trims(where: {body_trims: {body: {body_code: {_eq: $body_code}, version: {_eq: $version}}}}) {
    id
    hash
    created_at
    metadata
    updated_at
  }
}
"""


def fetch_trims_from_hasura(body_code: str, version: int) -> dict:
    gql = ResGraphQLClient()
    logger.info(f"fetch_trims_from_hasura body_code={body_code} version={version}")
    result = gql.query(
        gql_trim_by_body_code,
        variables={"body_code": body_code, "version": version},
    )["data"]["meta_trims"]
    logger.info(f"fetch_trims_from_hasura result={result}")
    if len(result) == 0:
        return None
    return result[0]


@res.flows.flow_node_attributes(
    memory="1Gi",
)
def sync_handler(event, context={}):
    """
     post to: https://data.resmagic.io/res-connect/flows/res-flow-node

        {
            "apiVersion": "resmagic.io/v1",
            "kind": "resFlow",
            "metadata": {
                "name": "meta.ONE.trims.sync_handler",
                "version": "primary",
                "data_bind_memory": false
            },
            "assets": [],
            "task": {
                "key": "trims.sync_handler-IDENTIFIER"
            },
            "args": {}
    }
    """
    run_sync()
    return {}


def run_sync():
    pg = PostgresConnector()
    ac = res.connectors.load("airtable")

    # Pull raw data from airtable
    logger.info("Downloading data from Airtable")
    boms_table = ac.get_table("appa7Sw0ML47cA8D1/tblnnt3vhPmPuBxAF")
    trim_taxonomy_table = ac.get_table("appa7Sw0ML47cA8D1/tblMheiAcYd8U42YD")
    material_category_table = ac.get_table("appa7Sw0ML47cA8D1/tblTppSkM6TpAnbOW")
    trims_table = ac.get_table("appa7Sw0ML47cA8D1/tbl3xL5qpJvsjROMs")

    boms_df = boms_table.to_dataframe(
        [
            "Name",
            "Code",
            "Category Number",
            "Type",
            "Material",
            "Body Number",
            "Fusing Usage",
            "Fusing",
            "Trim Code",
            "Trim Taxonomy 2.0",
            "Trim Quantity",
            "Trim Length",
            "Trim Length Unit of Measurement",
            "Cord Diameter",
            "Created At",
            "Last Updated At",
        ]
    )

    # bodies_df = bodies_table.to_dataframe()
    # trim_taxonomy_df = trim_taxonomy_table.to_dataframe()
    material_category_df = material_category_table.to_dataframe(
        [
            "Name",
            "Code",
            "Use",
            "Materials",
            "Sewing Cells",
            "Created At",
            "Updated At",
        ]
    )

    trim_taxonomy_df = trim_taxonomy_table.to_dataframe(
        [
            "TAXONOMY - PARENT",
            "STATION / CATEGORY",
            "Trim Type",
            "Trim Physical Property",
            "Trim Size",
            "Trim Material Content Structure",
            "Trim Secondary Component",
            "Trim",
            "Trim Material Content",
            "TRIM SUBTYPE",
            "Available in Bill of Materials",
            "Length's Unit of Measurement",
            "Last Updated At",
            "Created At",
        ]
    )

    trims_df = trims_table.to_dataframe()[["record_id", "Full Name"]]

    # Format data from BOMs
    df = boms_df.drop(
        columns=[
            "Name",
            "Last Updated At",
            "record_id",
            "__timestamp__",
            "Created At",
        ]
    )
    for column in [
        "Body Number",
        "Category Number",
        "Trim Code",
        "Code",
        "Trim Length Unit of Measurement",
        "Fusing Usage",
        "Fusing",
        "Material",
        "Type",
        "Trim Taxonomy 2.0",
    ]:
        df[column] = df[column].apply(pop_column)

    # Merge in other table data
    df = pd.merge(df, trims_df, left_on="Fusing", right_on="record_id", how="left")
    df = pd.merge(
        df,
        material_category_df[["record_id", "Name"]].rename(
            columns={"Name": "Material Name"}
        ),
        left_on="Material",
        right_on="record_id",
        how="left",
    )
    df = pd.merge(
        df,
        trim_taxonomy_df[
            [
                "record_id",
                "Trim",
                "STATION / CATEGORY",
                "Trim Type",
                "Trim Size",
                "Trim Material Content Structure",
                "Trim Secondary Component",
            ]
        ],
        left_on="Trim Taxonomy 2.0",
        right_on="record_id",
        how="left",
    )

    # Clean up
    df = df.rename(columns={"STATION / CATEGORY": "station"})
    df = df.drop(
        columns=[
            "record_id",
            "record_id_x",
            "record_id_y",
            "Fusing",
            "Material",
            "Trim Length",
            "Trim Quantity",
            "Trim Taxonomy 2.0",
        ]
    ).rename(columns={"Full Name": "Fusing"})
    df = df[df["Fusing"].isna()]

    # Create hash of record contents to allow for joins later
    hashes = []
    for i, row in df.iterrows():
        del row["Body Number"]
        hash = hashlib.md5(
            ", ".join(map(str, dict(row).values())).encode("ascii")
        ).hexdigest()
        hashes.append(hash)

    df["Hash"] = hashes

    # Final cleaning step
    final_trims_df = df.drop(columns=["Body Number"]).drop_duplicates()
    for column in final_trims_df.columns:
        final_trims_df[column] = final_trims_df[column].apply(null_column)

    # Build records to sync with db
    records = []
    for _, row in final_trims_df.iterrows():
        hash = row.pop("Hash")
        metadata = dict(row)
        for k, v in metadata.items():
            metadata[k] = null_column(v)
        records.append({"hash": hash, "metadata": metadata})

    # Perform sync
    logger.info("Syncing trims to Postgres")
    sync = PostgresDictSync(
        "meta.trims",
        records,
        on="hash",
        pg=pg,
        ignore_columns_for_update=["id", "created_at", "updated_at"],
    )
    sync.sync()

    # Build body_trims table
    meta_bodies_df = pg.run_query("select id, body_code from meta.bodies")
    meta_trims_df = pg.run_query("select id, hash from meta.trims")

    one_df = df.copy()
    one_df["Trim Quantity"] = boms_df["Trim Quantity"]
    one_df["Trim Length"] = boms_df["Trim Length"]
    one_df["record_id"] = boms_df["record_id"]
    two_df = pd.merge(
        one_df, meta_bodies_df, left_on="Body Number", right_on="body_code"
    )
    three_df = pd.merge(two_df, meta_trims_df, left_on="Hash", right_on="hash")

    records = []
    for _, row in (
        three_df[["id_x", "id_y", "Trim Length", "Trim Quantity", "record_id"]]
        .rename(columns={"id_x": "body_id", "id_y": "trim_id"})
        .iterrows()
    ):
        record = {
            "body_id": row["body_id"],
            "trim_id": row["trim_id"],
            "metadata": {
                "boms_airtable_record_id": row["record_id"]
                # 'Trim Length': row['Trim Length'],
                # 'Trim Quantity': row['Trim Quantity'],
            },
        }
        records.append(record)
    logger.info("Updating join between bodies and trims")
    pg.run_update("TRUNCATE meta.body_trims")
    pg.insert_records("meta.body_trims", records)
