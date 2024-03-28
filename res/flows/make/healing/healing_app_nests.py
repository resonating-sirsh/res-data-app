import res
import itertools
import pandas as pd

from res.airtable.healing import (
    REQUEST_PIECES,
    HEALING_ROLLS,
)
from res.airtable.print import NESTS
from res.flows import flow_node_attributes
from res.flows.make.production.healing import retry_create_healing_records
from res.utils import logger

# Updating this thing so it just retries making the request pieces if they arent there already.
# we can remove the healing app nests and all that other crapol and then eventually this thing
# since linkage can be done exclusively on the basis of the print app record id and piece name.


s3 = res.connectors.load("s3")


def get_requested_pieces_for_nested_pieces(piece_records, tries=0):
    asset_ids = piece_records.asset_id.unique()
    logger.info(f"Getting existing healing pieces for {asset_ids}")
    requested_pieces = REQUEST_PIECES.all(
        fields=[
            # TODO - when we fully switch over the flow we dont need to care about one code or piece name anymore.
            "ONE Code",
            "One Number",
            "Piece Name",
            "Material Code",
            "Is piece a Healing?",
            "Nested Pieces",
            "Print Asset ID (Print App)",
        ],
        formula=f"""AND(
            {{Material Code}}!='',
            {{Print Asset ID (Print App)}}!='',
            FIND({{Print Asset ID (Print App)}}, '{','.join(asset_ids)}')
        )""",
        sort=["Created"],
    )
    existing_pieces = set(
        (
            r["fields"]["Print Asset ID (Print App)"],
            r["fields"]["Piece Name"],
        )
        for r in requested_pieces
    )
    existing = piece_records.apply(
        lambda r: (r.asset_id, r.piece_name) in existing_pieces, axis=1
    )
    ids_to_reload = piece_records[~existing].asset_id.unique()
    logger.info(f"Reloading print assets in the healing app: {ids_to_reload}")
    if len(ids_to_reload) > 0 and tries == 0:
        for print_record_id in ids_to_reload:
            try:
                logger.info(
                    f"Retrying creating print asset {print_record_id} in the healing app."
                )
                retry_create_healing_records(print_record_id)
            except Exception as ex:
                logger.info(
                    f"Failed to generate healing piece records for {print_record_id}: {repr(ex)}"
                )
        return get_requested_pieces_for_nested_pieces(piece_records, tries=tries + 1)
    else:
        return requested_pieces


def create_healing_app_records(roll_name, nest_records):
    """
    The healing app is a totally seperate base -- but we want to keep track of nests and whats been assigned on them
    and where the pieces are.  The reason is so that we can do analysis about where on the roll problems are arising.
    Therefore we create nest records in the healing base, along with "piece" records associated with the nest.
    """
    logger.info(
        f"Creating healing app records for {roll_name} with nests {nest_records}"
    )
    s3 = res.connectors.load("s3")
    combined_df = pd.concat(
        [s3.read(r["fields"]["Dataframe Path"]) for r in nest_records]
    )
    combined_df = combined_df[
        combined_df.apply(
            lambda r: r.asset_id is not None and "::duplicate" not in r.piece_id, axis=1
        )
    ]
    get_requested_pieces_for_nested_pieces(combined_df)


def get_healing_roll_ids(roll_names):
    logger.info(f"Getting roll record ids for rolls: {roll_names}")
    records = HEALING_ROLLS.all(
        formula=f"""FIND({{Name}}, "{','.join(roll_names)}")""",
        fields=["Name"],
    )
    return {r["fields"]["Name"]: r["id"] for r in records}


@flow_node_attributes("healing_app_nests.handler", mapped=False)
def handler(event, context):
    recent_nests = NESTS.all(
        formula="""AND({Roll Name}!='', FIND({Current Roll Substaiton}, 'Print,Steam,Wash,Dry'))"""
        if not "roll" in event
        else f"""{{Roll Name}}='{event['roll']}'""",
        fields=[
            "Roll Name",
            "Rank",
            "Argo Jobkey",
            "Dataframe Path",
            "Nested Length in Yards",
        ],
        sort=["Roll Assigned At"],
    )
    roll_ids = get_healing_roll_ids(
        set([r["fields"]["Roll Name"] for r in recent_nests])
    )
    for roll_name, nest_records in itertools.groupby(
        sorted(recent_nests, key=lambda r: r["fields"]["Roll Name"]),
        key=lambda r: r["fields"]["Roll Name"],
    ):
        if not roll_name in roll_ids:
            logger.error(f"Roll {roll_name} not in the healing app")
            continue
        logger.info(f"Roll: {roll_name}, Nests: {nest_records}")
        try:
            create_healing_app_records(roll_name, nest_records)
        except Exception as ex:
            logger.info(f"Failed {roll_name}, {repr(ex)}")


# utility method to backfill nests
if __name__ == "__main__":
    handler({"roll": "R32189: CTW70"}, {})
