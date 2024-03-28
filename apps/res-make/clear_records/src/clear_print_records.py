import os
from pyairtable import Table

from res.utils import logger, secrets
from res.utils.env import boolean_envvar

DRY_RUN = boolean_envvar("DRY_RUN", 0)
MATERIALS = os.environ.get("MATERIALS", "all")


from data import (
    batch_delete_records,
    get_nests_with_missing_printfile,
    get_stale_unusable_nests,
    get_nests_with_canceled_ones,
    get_nests_with_unassignable_assets,
    get_duplicate_print_assets,
    get_old_nests,
    get_outdated_progressive_nests,
)


def initialize():
    if "AIRTABLE_API_KEY" not in os.environ:
        secrets.secrets_client.get_secret("AIRTABLE_API_KEY")


def clear_invalid_nests(cnx_nests, cnx_assets, materials=None):
    clearing_config = [
        {
            "reason": "older than we like",
            "kind": "nests",
            "getter": get_old_nests,
        },
        {
            "reason": "missing printfile",
            "kind": "nests",
            "getter": get_nests_with_missing_printfile,
        },
        {
            "reason": "contain canceled assets",
            "kind": "nests",
            "getter": get_nests_with_canceled_ones,
        },
        {
            "reason": "contain assets already printed",
            "kind": "nests",
            "getter": get_stale_unusable_nests,
        },
        {
            "reason": "contain unassignable assets",
            "kind": "nests",
            "getter": get_nests_with_unassignable_assets,
        },
        {
            "reason": "outdated progressive nest",
            "kind": "nests",
            "getter": get_outdated_progressive_nests,
        },
        {
            "reason": "duplicative with other assets",
            "kind": "assets",
            "getter": get_duplicate_print_assets,
        },
    ]

    connections = {
        "nests": cnx_nests,
        "assets": cnx_assets,
    }

    logger.info(
        "find invalid nests for %s" % ", ".join(materials)
        if materials
        else "all materials",
    )

    def records_to_delete(getter, kind, reason):
        logger.info(f"on the look out for {kind} that are {reason}")
        cnx = connections[kind]
        recs = getter(cnx, materials)
        rec_ct = len(recs)

        if rec_ct:
            logger.info(f"got {rec_ct} {kind} that are {reason}")
            batch_delete_records(
                cnx,
                recs,
                kind,
                reason,
                DRY_RUN,
            )
            logger.info(f"cleared {rec_ct} {kind} because theyre {reason}")
        else:
            logger.info(f"no old {kind} that are {reason}")

        return rec_ct or 0

    return {c["reason"]: records_to_delete(**c) for c in clearing_config}


if __name__ == "__main__":

    initialize()

    PRINT_BASE_ID = "apprcULXTWu33KFsh"

    if DRY_RUN:
        logger.info("dry running...")
    else:
        logger.info("running for realz...")

    cnx_nests = Table(
        os.environ.get("AIRTABLE_API_KEY"),
        PRINT_BASE_ID,
        "tbl7n4mKIXpjMnJ3i",
    )
    cnx_assets = Table(
        os.environ.get("AIRTABLE_API_KEY"), PRINT_BASE_ID, "tblwDQtDckvHKXO4w"
    )

    focus_materials = (
        [] if MATERIALS == "all" else [m.strip() for m in MATERIALS.split(",")]
    )

    clear_invalid_nests(cnx_nests, cnx_assets, focus_materials)
