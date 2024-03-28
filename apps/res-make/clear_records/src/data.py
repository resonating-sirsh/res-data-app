import os
from res.utils import logger
from res.utils.dataframes import dataframe_from_airtable, df_to_dict_of_lists

OLD_AGE_SECONDS = int(os.environ.get("OLD_AGE_SECONDS", 20 * 86400))


def join_filters(filters, glue="AND"):
    return "%s(%s)" % (glue, ",".join(filters)) if len(filters) else ""


def material_filter(materials_to_clear):
    if materials_to_clear:
        return [
            join_filters(["{Material Code}='%s'" % x for x in materials_to_clear], "OR")
        ]
    else:
        return []


def get_nests_with_canceled_ones(cnx_nests, materials=None):
    formula = ["{Rolls}=''", "{Has Canceled Print Asset}!=''"] + material_filter(
        materials
    )

    return get_nests(cnx_nests, formula)


def get_nests_with_missing_printfile(cnx_nests, materials=None):
    formula = ["{Is Stalled Nest}>0"] + material_filter(materials)

    return get_nests(cnx_nests, formula)


def get_old_nests(cnx_nests, materials=None):
    formula = [
        "{Rolls}=''",
        f"DATETIME_DIFF(NOW(), {{Created At}}, 's')>{OLD_AGE_SECONDS}",
    ] + material_filter(materials)

    return get_nests(cnx_nests, formula)


def get_nests_with_unassignable_assets(cnx_nests, materials=None):
    # give assignability a day long cool down just because assignable means
    # the thing is in at least one nest.  so if we are currently nesting this
    # nest then we dont need to care about this field.
    formula = [
        "{Rolls}=''",
        "{All Assets Assignable}=0",
        f"DATETIME_DIFF(NOW(), {{Created At}}, 's')>{24*3600}",
    ] + material_filter(materials)

    return get_nests(cnx_nests, formula)


def get_outdated_progressive_nests(cnx_nests, materials=None):
    # we produce progressive nests every 2 hours - so kill off old solutions that we didnt use.
    formula = [
        "{Rolls}=''",
        "{Print Queue}!='PRINTED'",
        "{Nesting Approach}='progressive'",
        f"DATETIME_DIFF(NOW(), {{Created At}}, 's')>{48*3600}",
    ] + material_filter(materials)

    return get_nests(cnx_nests, formula)


def get_stale_unusable_nests(cnx_nests, materials=None):
    formula = ["{Conflicting Nest Printed?}>0"] + material_filter(materials)

    return get_nests(cnx_nests, formula)


def sorter(x):
    print_queue_map = {"PRINTED": 16}
    return (
        print_queue_map.get(x["Print Queue"], 0)
        + 8 * bool(x["Assigned Printer v3"])
        + 4 * bool(x["Rolls Assignment v3"])
        + 2 * bool(len(x["Nested Asset Paths"]))
        + int(x["Digital Assets Ready"])
    )


def find_duplicates_by_key(assets):
    """
    expects a dataframe of print assets, finds duplicates based on a common key
    keeps the one thats farthest along in the print process
    """
    to_clear = []
    duplicate_groups = {
        k: v
        for k, v in df_to_dict_of_lists(assets.fillna(""), "Key").items()
        if len(v) > 1
    }
    for key, key_assets in duplicate_groups.items():
        sorted_assets = sorted(key_assets, key=sorter, reverse=True)
        # keep the one thats farthest along
        delete_candidates = sorted_assets[1:]
        logger.debug(
            f"keeping asset {sorted_assets[0]['id']} print queue: {sorted_assets[0]['Print Queue']}"
        )
        for c in delete_candidates:
            logger.debug(f"id: {c['id']} print queue: {c['Print Queue']}")
            if not c["Assigned Printer v3"] and not c["Rolls Assignment v3"]:
                to_clear.append(c)
    return to_clear


def get_duplicate_print_assets(cnx_print_assets, materials=None):

    formula = ""
    if int(os.environ["INCLUDE_HEALINGS"]) == 0:
        logger.info("Excluding healings from deduplication")
        formula = "AND({Rank}!='Healing')"

    fields = [
        "Key",
        "Nested Asset Paths",
        "Print Queue",
        "Rolls Assignment v3",
        "Assigned Printer v3",
        "Digital Assets Ready",
    ]
    logger.info("getting duplicate print assets")
    assets = dataframe_from_airtable(
        cnx_print_assets.all(formula=formula, fields=fields)
    )

    return [a["id"] for a in find_duplicates_by_key(assets)]


def get_nests(cnx_nests, filters):
    formula = join_filters(filters)
    logger.info(f"formula: {formula}")

    df = dataframe_from_airtable(
        cnx_nests.all(
            formula=formula,
            fields=[
                "Material Code",
                "Asset Ids",
                "Asset Set Id",
                "Key",
                "Assets",
                "Asset Path",
            ],
        )
    ).rename(columns={"id": "nest_record_id", "Asset Ids": "asset_keys"})

    return list(df.nest_record_id.unique())


def batch_delete_records(cnx, record_ids, kind, reason, dry_run=False):
    logger.info("about to delete %d nests for %s" % (len(record_ids), reason))

    for r in record_ids:
        logger.info(f"{kind}: {r}")

    if not dry_run:
        cnx.batch_delete(record_ids)
        logger.info(f"deleted {len(record_ids)} {kind}: {reason}")
    else:
        logger.info(f"would have deleted {len(record_ids)} records")
