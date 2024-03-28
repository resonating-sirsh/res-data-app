import res


from res.flows.make.production.piece_observation_utilities import (
    piece_observation_logging,
)

import os
import res.flows.make.production

from schemas.pydantic.make import *

from datetime import datetime, timedelta
import json

AIRTABLE_NAME = "One Order Responses"


class PieceObservationAirtableToHasura:
    def process_user_fields_airtable_to_hasura(hasura):
        res.utils.logger.warning(
            f"\n\n\n\nBeginning process_user_fields_airtable_to_hasura ..."
        )
        srv = res.connectors.airtable.AirtableConnector()

        sync_cache = res.connectors.load("redis")["AIRTABLE_CACHE"]["SYNC_DATES"]

        env = os.environ.get("RES_ENV", "development")

        def _make_akey(table_id, baseid, env, key):
            return f"{table_id}-{baseid}-{env}-{key}"

        key_name = "OneOrderResponse_LastSync_Hasura"
        last_sync_date = datetime(2022, 1, 1)

        attempt_sync = (
            datetime.utcnow()
        )  # log this for later, this will be our next sync time

        base_id = "appKwfiVbkdJGEqTm"
        if env == "production":
            base_id = "apps71RVe4YgfOO8q"

        key = _make_akey(AIRTABLE_NAME, base_id, env, key_name)

        last_sync_date_from_cache = sync_cache[key]
        if not last_sync_date_from_cache:
            last_sync_date_from_cache = last_sync_date

        base = srv[base_id]
        table_one_responses = base[AIRTABLE_NAME]

        iso_last_sync_date_from_cache = last_sync_date_from_cache.strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )

        filters = f"AND({{User Updated At}} > '{iso_last_sync_date_from_cache}', (OR({{User Contracts Failed}} != '', {{User Defects}} != '')) )"  # if we filter for rows only where these are set, we'll never capture where a defect was deleted?
        #
        oneresponses = table_one_responses.to_dataframe(
            fields=[
                "One Number",
                "User Defects",
                "User Contracts Failed",
                "User Updated At",
                "Pieces",
            ],
            filters=filters,
        )

        if oneresponses.empty:
            res.utils.logger.warning(
                f"\nNo User Defects or User Contract changes detected in One Order Responses since: {iso_last_sync_date_from_cache}"
            )
            piece_observation_logging.statsd_logger_airtable_zero_updates_from_user(
                AIRTABLE_NAME
            )

        else:
            res.utils.logger.warning(
                f"\n{len(oneresponses)} rows had User Defects or User Contract changes detected in One Order Responses since: {iso_last_sync_date_from_cache}"
            )
            piece_observation_logging.statsd_logger_airtable_updates_from_user(
                AIRTABLE_NAME, len(oneresponses)
            )

            def create_key_lookup_from_airtable(
                base, table_id, filter=None, env="development"
            ):
                fld = "Generated Piece Code" if (env == "production") else "Key"
                airtable_table = base[table_id]
                key_dict = airtable_table.to_dataframe(
                    fields=[fld, fld],
                    # you can't include record_id (airtable complains, but sends it if you dont ask), and you cant supply just one column name (AT complains)
                    filters=filter,
                ).to_dict("records")
                # if key_dict has key "Generated Piece Code"  then rename it to "Key"
                if "Generated Piece Code" in key_dict[0]:
                    key_dict = [
                        {
                            "Key": d["Generated Piece Code"],
                            "record_id": d["record_id"],
                        }
                        for d in key_dict
                    ]

                subset_dict = {d["record_id"]: d["Key"] for d in key_dict}
                return subset_dict

            res.utils.logger.warn(f"Loading Defects from Airtable - {base._base_id}...")
            defects_lookup = create_key_lookup_from_airtable(base, "Defects")
            res.utils.logger.warn(
                f"Loading Contracts table from airtable - {base._base_id}  - Contracts ..."
            )
            contracts_lookup = create_key_lookup_from_airtable(base, "Contracts")

            list_unique_pieces = oneresponses["Pieces"].explode().unique().tolist()
            # if len(list_unique_pieces) < 500:  # dont know what might be limit here
            pred = ",".join(
                [f"{{record_id}}='{recid}'" for recid in list_unique_pieces]
            )
            pred = f"OR({pred})"

            res.utils.logger.warn(
                f"Loading Pieces from Airtable - requesting {len(list_unique_pieces)} pieces ..."
            )
            pieces_lookup = create_key_lookup_from_airtable(
                base, "Piece Names", pred, env
            )

            res.utils.logger.warn(
                f"Pieces loading completed, rows: {len(pieces_lookup)} ..."
            )

            for index, row in oneresponses.iterrows():
                if isinstance(row.get("User Defects"), list):
                    row["User Defects"] = [
                        defects_lookup[d] for d in row["User Defects"]
                    ]
                if isinstance(row.get("User Contracts Failed"), list):
                    row["User Contracts Failed"] = [
                        contracts_lookup[d] for d in row["User Contracts Failed"]
                    ]
                if isinstance(row.get("Pieces"), list):
                    row["Pieces"] = [pieces_lookup[d] for d in row["Pieces"]]

            PieceObservationAirtableToHasura.build_mutation_for_update_and_exec(
                oneresponses, hasura
            )

        key = _make_akey(AIRTABLE_NAME, base._base_id, env, key_name)

        res.utils.logger.warn(
            f"Writing last sync time to redis key for future syncs key:{key}  datetime: {attempt_sync}"
        )
        sync_cache[key] = attempt_sync

        return

    def build_mutation_for_update_and_exec(oneresponses, hasura):
        from io import StringIO

        hasura = hasura or res.connectors.load("hasura")
        for index, row in oneresponses.iterrows():
            # right now we create the mutation at the level of the one number. THis is probably OK as this runs every X mins so there wont be more than 10 ones with updates I expect
            # if the ones were consistently in hasura (they arent, dont know why) then we could do this as one update across all ones with changes. TBD if its needed. i expect not.
            sio = StringIO()
            sio.write("mutation  update_many_make_one_pieces {")
            sio.write("\n update_make_one_pieces_many (")
            sio.write("\n  updates: [")

            for piececode in row["Pieces"]:
                # We update each piece in make_one_pieces with the defects. In future, a given piece might own the defect. Right now, a defect at the one applies to all pieces
                # build the piece_oid
                piece_oid = res.utils.uuid_str_from_dict(
                    {
                        "one_number": int(float(row["One Number"])),
                        "piece_code": piececode,
                    }
                )

                # these need to be json converted from Airtable to Hasura
                userDefects = row.get("User Defects")

                user_contracts_failed = row.get("User Contracts Failed")

                userDefects = json.dumps(userDefects)
                user_contracts_failed = json.dumps(user_contracts_failed)
                if userDefects == "NaN":
                    userDefects = "[]"

                if user_contracts_failed == "NaN":
                    user_contracts_failed = "[]"

                # build the where and set parts of the update_many
                sio.write("\n  { ")
                sio.write(f'\n   where: {{ oid: {{ _eq: "{piece_oid}" }} }},')
                sio.write(
                    f"\n  _set: {{ user_defects: {userDefects}, user_contracts_failed: {user_contracts_failed} }}"
                )
                sio.write("\n  } , ")

            finish = """\n\n ]
                ) {
                    affected_rows
                }
                }"""

            sio.write(finish)

            result = sio.getvalue()

            res.utils.logger.debug("\nUsing the following mutation to update:{result}")
            res.utils.logger.warn(
                f"Found user defects or user contracts failed for one number: {row['One Number']}, updating to hasura make_one_pieces, pieces count: {len(row['Pieces'])}"
            )

            try:
                retval = hasura.tenacious_execute_with_kwargs(result)
                res.utils.logger.warn(
                    f"Hasura update succeeded for  {row['One Number']}"
                )

                piece_observation_logging.statsd_logger_airtable_updates_saved_to_hasura(
                    True
                )

            except Exception as ex:
                res.utils.logger.warn(
                    f"\n\nError updating hasura make_one_pieces for One Number: { row['One Number'] }  Exception: {ex}"
                )
                piece_observation_logging.statsd_logger_airtable_updates_saved_to_hasura(
                    False
                )
