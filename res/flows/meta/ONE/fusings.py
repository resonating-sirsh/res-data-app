import pandas as pd
import res.connectors
from res.connectors.postgres.PostgresConnector import PostgresConnector
from res.utils.dicts import index_dicts, group_by
from res.utils.logging import logger

pop_column = lambda x: x[0] if isinstance(x, list) else x


import res
from res.flows.meta.ONE.trims import run_sync


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
                "name": "meta.ONE.fusings.sync_handler",
                "version": "primary",
                "data_bind_memory": false
            },
            "assets": [],
            "task": {
                "key": "fusings.sync_handler-IDENTIFIER"
            },
            "args": {}
    }
    """
    run_sync()
    return {}


def run_sync():
    pg = PostgresConnector()
    ac = res.connectors.load("airtable")

    logger.info("Fetching fusings from Airtable")
    boms_table = ac.get_table("appa7Sw0ML47cA8D1/tblnnt3vhPmPuBxAF")
    body_pieces_table = ac.get_table("appa7Sw0ML47cA8D1/tbl4V0x9Muo2puF8M")
    trim_table = ac.get_table("appa7Sw0ML47cA8D1/tbl3xL5qpJvsjROMs")

    body_pieces_df = body_pieces_table.to_dataframe(
        ["Generated Piece Code", "Generated Piece Name", "Body Number (from Bodies)"]
    )
    boms_df = boms_table.to_dataframe(
        [
            "Name",
            "Body Pieces",
            "Body Number",
            "Fusing Usage",
            "Fusing",
        ]
    )

    trim_df = trim_table.to_dataframe()

    boms_df["Fusing"] = boms_df["Fusing"].apply(pop_column)
    boms_df["Body Number"] = boms_df["Body Number"].apply(pop_column)
    boms_df = boms_df[(~boms_df["Fusing"].isna()) & (~boms_df["Body Pieces"].isna())]

    body_fusings_df = pd.merge(
        boms_df[["Body Number", "Fusing", "Body Pieces"]],
        trim_df[["Code", "Full Name", "Material Name", "Name", "record_id"]],
        left_on="Fusing",
        right_on="record_id",
    )
    body_fusings_df["Material Name"] = body_fusings_df["Material Name"].apply(
        pop_column
    )

    body_pieces_by_id = index_dicts(
        body_pieces_df.to_dict("records"), lambda d: d["record_id"]
    )

    fusings = []
    for _, row in body_fusings_df.iterrows():
        for record_id in row["Body Pieces"]:
            body_piece = body_pieces_by_id[record_id]
            fusings.append(
                {
                    "body_code": row["Body Number"],
                    "fusing": row["Name"],
                    "fusing_code": row["Code"],
                    # "piece_name": body_piece['Generated Piece Name'],
                    "piece_code": body_piece["Generated Piece Code"],
                }
            )

    grouped_fusings = group_by(fusings, "body_code", keep_key=False)

    logger.info("Updating fusings in Postgres")
    for body_code, fusing in grouped_fusings.items():
        set_ = {"fusing_pieces": fusing}
        where = {"body_code": body_code}
        pg.update_records("meta.bodies", set_, where)
