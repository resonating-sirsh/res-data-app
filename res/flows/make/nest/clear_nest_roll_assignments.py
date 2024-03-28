import json
import os
import res
import traceback as tb

from res.airtable.print import ASSETS, ROLLS, PRINTFILES, NESTS
from res.utils import logger
from res.utils.airtable import multi_option_filter
from .progressive.utils import post_unassignment_observations_for_nest_by_id


RES_ENV = os.getenv("RES_ENV", "development")

PRINT_BASE = "apprcULXTWu33KFsh"
ROLLS_TABLE = "tblSYU8a71UgbQve4"
PRINTFILES_TABLE = "tblAQcPuKUDVfU7Fx"
NESTS_TABLE = "tbl7n4mKIXpjMnJ3i"
PRINT_ASSETS_TABLE = "tblwDQtDckvHKXO4w"


ROLL_FIELDS = [
    "Assets Pending to Print",
    "Asset Sets Pending to Print",
    "Material Code",
    "Name",
]


def select_where(lhs, *, is_not_empty: bool = True):
    return f"{lhs}!=''" if is_not_empty else f"{lhs}=''"


def rolls_with(*, materials: list, names: list):
    filter_formula = "AND(%s)" % ",".join(
        multi_option_filter("Material Code", materials)
        + multi_option_filter("Name", names)
    )

    return ROLLS.all(formula=filter_formula, fields=ROLL_FIELDS)


def extract_ids(rollup_field):
    if not rollup_field:
        return []
    return [y.strip() for y in rollup_field.split(",")]


def get_print_files_to_update(roll_record_id, open_nest_ids):
    print_file_updates = []
    try:
        formula = "AND(%s)" % ",".join(
            [
                f"{{__roll_record_id}}='{roll_record_id}'",
                f"{{Print Queue}}!='PRINTED'",
                f"{{Print Queue}}!='CANCELED'",
            ]
            + multi_option_filter(
                "__nest_record_ids", open_nest_ids, is_list_column=True
            )
        )
        open_print_files_on_roll = PRINTFILES.all(
            formula=formula,
            fields=["Nests", "Assigned Printer"],
        )
        logger.info(f"{len(open_print_files_on_roll)} print file(s) were not printed")
        for pf in open_print_files_on_roll:
            linked_nests = pf["fields"].get("Nests", [])
            assigned_printer = pf["fields"].get("Assigned Printer", None)
            unprinted_nest_ids = set(open_nest_ids) & set(linked_nests)
            printed_nest_ids = set(linked_nests) - unprinted_nest_ids
            if assigned_printer is None:
                cancellation_reason = (
                    "NEST-ROLL ASSIGNMENT CLEARED BEFORE PRINTER ASSIGNED"
                )
            else:
                cancellation_reason = (
                    "NEST-ROLL ASSIGNMENT CLEARED AFTER PRINTER ASSIGNED"
                )
            print_file_updates.append(
                {
                    "id": pf["id"],
                    "fields": {
                        "Nests": list(printed_nest_ids),
                        "Unprinted Nests": ",".join(unprinted_nest_ids),
                        "Print Queue": "CANCELED",
                        "Assigned Printer": None,
                        "Unassigned Printer": assigned_printer,
                        "Cancellation Bot / Reason": cancellation_reason,
                    },
                }
            )
    except Exception as e:
        logger.error(f"Error trying to build Print File updates! {e!r}")
        tb.print_exc(limit=5)

    return print_file_updates


def logging_message(updates, record_type, description, op, verb, infl_verb=None):
    infl_verb = verb if infl_verb is None else infl_verb
    if record_type[-1] != "s":
        record_type = f"{record_type}s"
    if len(updates) == 1:
        inflected = f"{record_type[:-1]} {infl_verb}"
    else:
        inflected = f"{record_type} {verb}"
    logger.info(f"{op} {record_type}... {len(updates)} {inflected} {description}")
    logger.info(json.dumps(updates))


def log_dry_run_message(updates, record_type, description):
    logging_message(updates, record_type, description, "Would update", "would be")


def log_update_message(updates, record_type, description):
    logging_message(
        updates, record_type, description, "Updating", "are being", "is being"
    )


def clear_assets(roll_record_id, asset_updates, dry_run):
    record_type = "Assets"
    description = f"unassigned from roll {roll_record_id}"
    if dry_run:
        log_dry_run_message(asset_updates, record_type, description)
    else:
        log_update_message(asset_updates, record_type, description)
        try:
            ASSETS.batch_update(
                asset_updates,
                typecast=True,
            )
        except Exception as e:
            logger.error(f"Error clearing assets! {e!r}")
            tb.print_exc(limit=5)


def clear_nests(roll_record_id, nest_updates, dry_run):
    record_type = "Nests"
    description = f"unassigned from roll {roll_record_id}"
    if dry_run:
        log_dry_run_message(nest_updates, record_type, description)
    else:
        log_update_message(nest_updates, record_type, description)
        try:
            NESTS.batch_update(
                nest_updates,
                typecast=True,
            )
        except Exception as e:
            logger.error(f"Error clearing nests! {e!r}")
            tb.print_exc(limit=5)


def cancel_print_files(print_file_updates, dry_run):
    record_type = "Print Files"
    description = "canceled and unlinked from unprinted nests and any assigned printer"
    if dry_run:
        log_dry_run_message(print_file_updates, record_type, description)
    else:
        log_update_message(print_file_updates, record_type, description)
        try:
            PRINTFILES.batch_update(
                print_file_updates,
                typecast=True,
            )
        except Exception as e:
            logger.error(f"Error canceling print files! {e!r}")
            tb.print_exc(limit=5)
        # try to cancel the hasura printfiles
        hasura = res.connectors.load("hasura")
        for r in print_file_updates:
            try:
                hasura.execute(
                    """
                    mutation ($id: String) {
                        update_make_printfile(where: {airtable_record_id: {_eq: $id}}, _set: {state: "CANCELLED"}) {
                            returning {
                                name
                            }
                        }
                    }
                    """,
                    {"id": r["id"]},
                )
            except Exception as e:
                logger.error(f"Failed to update hasura printfile: {repr(e)}")


def update_roll(roll_record_id, roll_update, dry_run):
    record_type = "Rolls"
    description = f"Cleared nest-roll assignment data from roll {roll_record_id}"

    if dry_run:
        log_dry_run_message(roll_update, record_type, description)
    else:
        log_update_message(roll_update, record_type, description)

        try:
            ROLLS.update(roll_record_id, roll_update)
        except Exception as e:
            logger.error(f"Error clearing nest-roll assignment from roll! {e!r}")
            tb.print_exc(limit=5)


def handle_linked_records(roll_record_id, roll, asset_contract_variables, dry_run):
    open_asset_ids = extract_ids(roll["fields"].get("Assets Pending to Print"))
    logger.info(f"{len(open_asset_ids)} asset(s) were not printed")
    assets_existing_cv = {}
    if len(asset_contract_variables) > 0:
        assets_existing_cv = {
            r["id"]: r["fields"].get("Material - Contract Variable", [])
            for r in ASSETS.all(
                fields=["Material - Contract Variable"],
                formula=f"FIND({{_record_id}}, '{','.join(open_asset_ids)}')",
            )
        }
    asset_updates = [
        {
            "id": asset_id,
            "fields": {
                "Print Queue": "TO DO",
                "Assigned Printer v3": None,
                "Roll Rank v3": None,
                "Rolls Assignment v3": None,
                "Utilized Roll": "",
                **(
                    {
                        "Material - Contract Variable": list(
                            set(
                                asset_contract_variables
                                + assets_existing_cv.get(asset_id, [])
                            )
                        )
                    }
                    if len(asset_contract_variables)
                    else {}
                ),
            },
        }
        for asset_id in open_asset_ids
    ]
    open_nest_ids = extract_ids(roll["fields"].get("Asset Sets Pending to Print"))
    logger.info(f"{len(open_nest_ids)} nest(s) was(were) not printed")
    nest_updates = [
        {
            "id": nest_id,
            "fields": {
                "Rolls": [],
                "Eligible for Assignment": True,
                "Print Queue": "TO DO",
                "Print Asset Downloaded?": False,
                "Are Sets Assigned to Rolls?": False,
                "Print Queue Mirror": None,
                "File Path in Local Machine": None,
                "Asset File Status": None,
                "Assigned Printer": None,
                "Print File": [],
            },
        }
        for nest_id in open_nest_ids
    ]

    print_file_updates = get_print_files_to_update(roll_record_id, open_nest_ids)

    if asset_updates:
        clear_assets(roll_record_id, asset_updates, dry_run)

    if nest_updates:
        try:
            res.connectors.load("hasura").execute(
                """
                mutation ($assigned_roll_name: String, $nest_keys: [String!]) {
                    update_make_nests_many(updates: {where: {nest_key: {_in: $nest_keys}}, _set: {assigned_roll_name: $assigned_roll_name}}) {
                        affected_rows
                    }
                }
                """,
                {
                    "assigned_roll_name": "",
                    "nest_keys": [
                        n["fields"]["Argo Jobkey"]
                        for n in NESTS.all(
                            fields=["Argo Jobkey"],
                            formula=f"{{Roll Name}}='{roll['fields']['Name']}'",
                        )
                    ],
                },
            )
            for id in open_nest_ids:
                post_unassignment_observations_for_nest_by_id(
                    roll["fields"]["Name"], id
                )
        except Exception as ex:
            logger.warn(f"Failed to update hasura nests with assignment: {repr(ex)}")

        clear_nests(roll_record_id, nest_updates, dry_run)

    if print_file_updates:
        cancel_print_files(print_file_updates, dry_run)

    return len(asset_updates), len(nest_updates), len(print_file_updates)


def handle_roll(roll_record_id, roll_name, deallocate_ppu, dry_run, notes):
    roll_update = {
        "Ready for Print": False,
        "Print Priority": None,
        "Assigned Yards v3": None,
        "Available Yards v3": None,
        "Assigned Printer Name v3": None,
        "Assigned Assets v3": None,
        "Print Flow": None,
        "Manual Pick Pretreat": False,
        "Schedule to pretreat": False,
        "Picking  Status pretreat": None,
    }

    if deallocate_ppu:
        roll_update["Allocated for PPU"] = False
        roll_update["Cut To Length (yards)"] = None
        try:
            from res.learn.optimization.schedule.ppu import update_ppu_def

            update_ppu_def(roll_name)
        except:
            pass

    if notes is not None:
        roll_update["Flag for Review Reason"] = notes

    update_roll(roll_record_id, roll_update, dry_run)


def clear_nest_roll_assignments(
    materials,
    rolls,
    dry_run,
    notes=None,
    asset_contract_variables=[],
    deallocate_ppu=True,
):
    logger.info(
        f"finding nest-roll assignments to clear for {'materials: ' + ','.join(materials)}"
    )
    if rolls:
        logger.info(f"limiting to the following rolls: {', '.join(rolls)}")
    total_assets, total_nests, total_pfs = 0, 0, 0
    for roll in rolls_with(materials=materials, names=rolls):
        try:
            assets, nests, pfs = handle_linked_records(
                roll["id"], roll, asset_contract_variables, dry_run
            )
            total_assets += assets
            total_nests += nests
            total_pfs += pfs
            handle_roll(
                roll["id"], roll["fields"]["Name"], deallocate_ppu, dry_run, notes
            )
        except Exception as e:
            logger.error(f"There was an issue handling roll {roll['id']} {e!r}")
            tb.print_exc()
    return total_assets, total_nests, total_pfs
