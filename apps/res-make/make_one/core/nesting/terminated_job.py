# utility methods to deal with terminated print jobs.
import res
import requests
from res.airtable.healing import PIECES_WITH_ZONES, ZONE_X_DEFECT
from res.airtable.print import PRINTFILES as PRINTFILES_TABLE
from res.utils import logger, ping_slack


def mark_partial_printing_in_healing_app(printfile_id):
    pf = PRINTFILES_TABLE.get("rec191yCOwN9HfmaL")
    roll_name = pf["fields"]["Roll Name"]
    fraction_printed = (pf["fields"]["printed_length"] - 300) / float(
        pf["fields"]["requested_length"]
    )
    logger.info(
        f"Printfile {printfile_id} was printed to frac {fraction_printed:.3f} on {roll_name}."
    )
    manifest = pf["fields"]["Asset Path"] + "/manifest.feather"
    df = res.connectors.load("s3").read(manifest)
    df["piece_bottom_frac"] = (
        df["inches_from_printfile_top"] + df["max_y_inches"] - df["min_y_inches"]
    ) / df["printfile_height_inches"]
    # figure out what was either partially printed or completely missed.
    need_healing = df[df["piece_bottom_frac"] > fraction_printed].sort_values(
        "inches_from_printfile_top"
    )
    need_healing["piece_code"] = need_healing["piece_name"].apply(
        lambda n: "-".join(n.split("-")[-2:])
    )
    # manifest might be missing the zone info (TODO - just propagate it in from the nest)
    zone_info = {
        r["fields"]["Generated Piece Code"]: r["fields"]
        for r in PIECES_WITH_ZONES.all(
            fields=[
                "Generated Piece Code",
                "Generated Piece Name",
                "Commercial Acceptability Zone (from Part Tag)",
            ],
            formula=f"AND({{Generated Piece Code}}!='', find({{Generated Piece Code}}, '{','.join(set(n for n in need_healing.piece_code.values if n is not None))}'))",
        )
    }
    need_healing["zone"] = need_healing["piece_code"].apply(
        lambda c: zone_info.get(c, {}).get(
            "Commercial Acceptability Zone (from Part Tag)", [""]
        )[0]
    )
    # find the defects corresponding to partial printing
    defects = ZONE_X_DEFECT.all(
        fields=["KEY", "Zone"], formula="FIND('Partially Printed', {KEY})"
    )
    # add the defects directly to the hasura db as the healing interface would do.
    payload = []
    for _, r in need_healing.iterrows():
        defect = next(d for d in defects if d["fields"]["Zone"] == r["zone"])
        for inspector in [1, 2]:
            payload.append(
                {
                    "challenge": False,
                    "airtable_defect_id": defect["id"],
                    "defect_idx": 0,
                    "defect_name": defect["fields"]["KEY"],
                    "fail": True,
                    "inspector": inspector,
                    "nest_key": r["nest_job_key"],
                    "piece_id": r["piece_id"],
                    "image_idx": 2,
                }
            )
    result = res.connectors.load("hasura").execute(
        """
        mutation ($objects: [make_roll_inspection_insert_input!]!) {
            insert_make_roll_inspection(
            objects: $objects,
            on_conflict: {
                constraint: roll_inspection_pkey,
                update_columns: [fail, challenge, defect_id, defect_name, image_idx]
            }
            ){ 
            affected_rows
            }
        }
        """,
        {"objects": payload},
    )
    # we may not want to sync to airtable yet
    # for inspector in [1, 2]:
    #     requests.post(
    #         "https://data.resmagic.io/res-connect/roll_inspection_to_airtable",
    #         json={"inspector": inspector, "roll_key": roll_name.replace(": ", "-")},
    #     )
    ping_slack(
        f"[HEALING] Marked {len(payload)} pieces as partially printed on healing app for {roll_name} (printfile {printfile_id}).",
        "autobots",
    )
    return result
