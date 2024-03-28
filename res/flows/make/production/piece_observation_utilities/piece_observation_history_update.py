import res
from dateutil import parser


from res.flows.make.production.piece_observation_utilities import (
    piece_observation_logging,
)


from res.flows.api import FlowAPI

import os
import res.flows.make.production
import pytz

from schemas.pydantic.make import *

from datetime import datetime, timedelta
import json


GET_PIECES_HISTORY_QUERY = """query GetPiecesHistory($pieceoids: [uuid!]!) {
    make_one_pieces_history(where: {piece_oid: {_in: $pieceoids}}) {
        id
        piece_id
        piece_oid
        observed_at
        node_id
        node_status
        set_key
        node {
        name
        }
        piece {
        code
        }
    
    }
    }    
        """

MUTATION_INSERT_PIECES_HISTORY = """ mutation InsertAndGetPiecesHistory($objects: [make_one_pieces_history_insert_input!]!) {
        insert_make_one_pieces_history(objects: $objects, on_conflict: {constraint: one_pieces_history_pkey, update_columns: [id, piece_id, piece_oid, observed_at, node_id, set_key,  printer_file_id, roll_name, nest_key, roll_key, print_job_name
        ,material_code
        ,metadata
        ,make_instance
        ,contracts_failed
        ,defects   
        ,user_defects
        ,user_contracts_failed]}){
        affected_rows
        returning {
        id
        piece_id
        piece_oid
        observed_at
        node_id
        node_status
        set_key
        printer_file_id
        roll_name
        nest_key
        roll_key
        print_job_name
        material_code
        metadata
        make_instance
        contracts_failed
        defects
        user_defects
        user_contracts_failed
        }
    
    }    
    } """


class PieceObservationHistoryUpdate:
    # , on_conflict: {constraint: make_one_pieces_history_pkey, update_columns: []}, returning: { where: {piece_oid: {_in: $pieceoids}}, columns: [id, piece_id, piece_oid, observed_at, node_id, node_status, set_key] }
    def onepiece_node_history_update(payload: OnePieceSetUpdateRequest, hasura):
        list_dicts = [dict(p) for p in payload.pieces]
        list_props_to_keep = [
            "id",
            "piece_id",
            "piece_oid",
            "observed_at",
            "node_id",
            "node_status",
            "set_key",
            "print_file_id",
            "printer_file_id",
            "roll_name",
            "nest_key",
            "roll_key",
            "printjob_name",
            "print_job_name",
            "material_code",
            "metadata",
            "make_instance",
            "contracts_failed",
            "defects",
        ]

        for d in list_dicts:
            d["set_key"] = (
                payload.metadata.get("nest_key")
                or payload.metadata.get("print_file_id")
                or payload.metadata.get("roll_key")
                #  or payload.metadata.get("material_code")
            )

            d["node_status"] = d.pop("status")
            d["piece_oid"] = d.pop("oid")
            d["id"] = uuid_str_from_dict(
                {
                    "piece_id": d.get("piece_id"),
                    "piece_oid": d.get("piece_oid"),
                    "observed_at": d.get("observed_at"),
                    "node_id": d.get("node_id"),
                    "node_status": d.get("node_status"),
                    "print_file_id": d.get("print_file_id"),
                    "printer_file_id": d.get("printer_file_id"),
                    "set_key": d.get("set_key"),
                    "roll_name": d.get("roll_name"),
                    "nest_key": d.get("nest_key"),
                    "roll_key": d.get("roll_key"),
                    "printjob_name": d.get("printjob_name"),
                    "print_job_name": d.get("print_job_name"),
                    "material_code": d.get("material_code"),
                    "make_instance": d.get("make_instance"),
                    "defects": d.get("defects"),
                    "contracts_failed": d.get("contracts_failed"),
                    "user_defects": d.get("user_defects"),
                    "user_contracts_failed": d.get("user_contracts_failed"),
                }
            )
            for k in list(d.keys()):
                if k not in list_props_to_keep:
                    d.pop(k)

        # rename these to match the DB and mutation
        if d.get("print_file_id"):
            d["printer_file_id"] = d.pop("print_file_id")
        if d.get("printjob_name"):
            d["print_job_name"] = d.pop("printjob_name")

        pieceoids = [d.get("piece_oid") for d in list_dicts]

        retval1 = hasura.tenacious_execute_with_kwargs(
            MUTATION_INSERT_PIECES_HISTORY, objects=list_dicts
        )
        # this ideally should be a round trip but is super hard to get workign with hasura
        retval2 = hasura.tenacious_execute_with_kwargs(
            GET_PIECES_HISTORY_QUERY, pieceoids=pieceoids
        )

        if len(list_dicts) > 1:
            res.utils.logger.info(
                f"piece_observation: {len(list_dicts)} pieces updated in history table"
            )
        if len(retval2.get("make_one_pieces_history")) > 1:
            res.utils.logger.info(
                f"piece_observation: {len(retval2.get('make_one_pieces_history'))} pieces returned from history table"
            )

            listupdates = retval2["make_one_pieces_history"]
            list_pieces_valid_delta = []
            for d in list_dicts:
                updates_for_piece = [
                    update
                    for update in listupdates
                    if update["piece_oid"] == d["piece_oid"]
                ]
                # find the most recent in filtered_updates based on observed_at
                for update in updates_for_piece:
                    update["observed_at"] = parser.parse(update["observed_at"]).replace(
                        tzinfo=None
                    )

                list.sort(
                    updates_for_piece, key=lambda x: x["observed_at"], reverse=True
                )

                if len(updates_for_piece) > 1:
                    # this is complex.  we cannot guarantee that an update written to hasura is the most recent observed update. It could an older update being injected somewhere in the history of the piece
                    # thus, we find where we updated, and call that update the matching_piece_update.  We then check of earlier (farther back in time) piece updates, see if that has change between the one we injected and the more recent. fire an event if so
                    # we also do the more standard thing, and check the later (most recent in time update, called "later update") and fire an event if that caused a delta to happen.

                    # get current index of update matching current payload.observed at - i.e find our current update in the ordered by date list
                    payload_observed_at_tzfree = parser.parse(
                        payload.observed_at
                    ).replace(tzinfo=None)

                    matching_piece_update_list = [
                        i
                        for i, u in enumerate(updates_for_piece)
                        if u["observed_at"] == payload_observed_at_tzfree
                    ]
                    if matching_piece_update_list:  # Check if the list is not empty
                        matching_index = matching_piece_update_list[0]
                        matching_piece_update = updates_for_piece[matching_index]

                        earlier_update_node_status = None
                        earlier_update_node = None
                        later_update_node = None
                        later_update_node_status = None

                        # check if update after this one gives us a node delta event
                        if len(updates_for_piece) > matching_index + 1:
                            earlier_update = updates_for_piece[matching_index + 1]
                            earlier_update_node = (
                                earlier_update.get("node")
                                if earlier_update is not None
                                else None
                            )
                            earlier_update_node_status = (
                                earlier_update.get("node_status")
                                if earlier_update is not None
                                else None
                            )
                        # check if earlier update is legal
                        if matching_index - 1 >= 0:
                            later_update = updates_for_piece[matching_index - 1]
                            later_update_node = (
                                later_update.get("node")
                                if later_update is not None
                                else None
                            )
                            later_update_node_status = (
                                later_update.get("node_status")
                                if later_update is not None
                                else None
                            )

                        if (
                            (
                                (matching_piece_update["node_status"])
                                != earlier_update_node_status
                            )
                            and (earlier_update_node_status is not None)
                        ) or (
                            (earlier_update_node) != matching_piece_update.get("node")
                            and earlier_update_node is not None
                        ):
                            current_timestamp = matching_piece_update["observed_at"]
                            previous_timestamp = earlier_update["observed_at"]
                            diff = current_timestamp - previous_timestamp
                            # piece finished at node, injected update was the later, most recent update (it was the after state)
                            type_of_delta = (
                                "node_status_changed"
                                if matching_piece_update["node"]
                                == earlier_update["node"]
                                else "node_changed"
                            )

                            piece_finished_at_node_update = {
                                "piece_id": "",
                                "piece_oid": d["piece_oid"],
                                "code": matching_piece_update["piece"]["code"],
                                "current_observed_at": str(current_timestamp),
                                "previous_observed_at": str(previous_timestamp),
                                "type_of_delta": type_of_delta,
                                "current_node": matching_piece_update["node"]["name"],
                                "previous_node": earlier_update["node"]["name"],
                                "current_status": matching_piece_update["node_status"],
                                "previous_status": earlier_update["node_status"],
                                "time_delta": str(diff),
                            }

                            list_pieces_valid_delta.append(
                                piece_finished_at_node_update
                            )

                            res.utils.logger.warning(
                                f"piece finished at node, injected update was the later, most recent update (it was the after state) : {json.dumps(piece_finished_at_node_update, indent=4, sort_keys=True)}"
                            )

                        if (
                            (
                                (matching_piece_update["node_status"])
                                != later_update_node_status
                            )
                            and (later_update_node_status is not None)
                        ) or (
                            (later_update_node) != matching_piece_update.get("node")
                            and later_update_node is not None
                        ):
                            # "piece finished at node, injected update was earlier update (it was the before state)
                            current_timestamp = later_update["observed_at"]
                            previous_timestamp = matching_piece_update["observed_at"]
                            diff = current_timestamp - previous_timestamp

                            type_of_delta = (
                                "node_status_changed"
                                if later_update["node"] == matching_piece_update["node"]
                                else "node_changed"
                            )

                            piece_finished_at_node_update = {
                                "piece_id": "",
                                "piece_oid": d["piece_oid"],
                                "code": later_update["piece"]["code"],
                                "current_observed_at": str(current_timestamp),
                                "previous_observed_at": str(previous_timestamp),
                                "type_of_delta": type_of_delta,
                                "current_node": later_update["node"]["name"],
                                "previous_node": matching_piece_update["node"]["name"],
                                "current_status": later_update["node_status"],
                                "previous_status": matching_piece_update["node_status"],
                                "time_delta": str(diff),
                            }

                            list_pieces_valid_delta.append(
                                piece_finished_at_node_update
                            )

                            res.utils.logger.warning(
                                f"piece finished at node, injected update was earlier update (it was the before state) : {json.dumps(piece_finished_at_node_update, indent=4, sort_keys=True)}"
                            )

            if list_pieces_valid_delta:
                # os.environ["KAFKA_SCHEMA_REGISTRY_URL"] = "localhost:8001" # local
                # os.environ['KAFKA_KGATEWAY_URL'] = "https://data.resmagic.io/kgateway/submitevent"
                # os.environ[
                #     "KAFKA_KGATEWAY_URL"
                #   ] = "https://data.resmagic.io/kgateway/submitevent" # uncomment on local

                TOPIC = "res_make.piece_tracking.make_piece_observation_node_deltas"
                kafka = res.connectors.load("kafka")

                import uuid

                hash_id = uuid_str_from_dict({"listdeltas": list_pieces_valid_delta})
                kafMessage = {
                    "id": f"oc-{payload.one_code}-one-{payload.one_number}-ob-{payload.observed_at}-{payload.node}-{hash_id[:8]}",
                    "one_code": payload.one_code,
                    "node": payload.node,
                    "one_number": payload.one_number,
                    "metadata": payload.metadata,
                    "pieces": list_pieces_valid_delta,
                }

                res.utils.logger.warning(
                    f"Publishing Kafka Node Delta to make_piece_observation_node_deltas for {payload.one_number}"
                )

                kafka[TOPIC].publish(kafMessage)  # use kgateway if local

                # format kafka message into pretty json for printing
                kafMessagePretty = json.dumps(kafMessage, indent=4, sort_keys=True)
                res.utils.logger.warning(f"Published: {kafMessagePretty}")
