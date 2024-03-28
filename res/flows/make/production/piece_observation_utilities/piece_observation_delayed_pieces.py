import res
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

# currently node 3 = generally make print roll inspection is considered to be the last node, so we omit those that are in statsu exit there
# 3f9f9263-630e-eb35-805f-4ed18f847289 = cutter
GET_DELAYED_PIECES_Q = """
    query MyQuery($nodeid1: uuid = "", $date1: timestamptz = "", $nodeid2: uuid = "", $date2: timestamptz = "", $nodeid3: uuid = "", $date3: timestamptz = "", $nodeid4: uuid = "", $date4: timestamptz = "", $early: timestamptz = "") {
    make_one_pieces(where: { 
                    _and: [
    {
        _or: [
            {_and: [
                {_and: [
                    {observed_at: {_lt: $date1}},
                    {observed_at: {_gt: $early}}
                ]},
                {node_id: {_eq: $nodeid1}}
            ]},
            {_and: [
                {_and: [
                    {observed_at: {_lt: $date2}},
                    {observed_at: {_gt: $early}}
                ]},
                {node_id: {_eq: $nodeid2}}
            ]},
            {_and: [
                {_and: [
                    {observed_at: {_lt: $date3}},
                    {observed_at: {_gt: $early}}
                ]},
                {node_id: {_eq: $nodeid3}}
            ]},
            {_and: [
                {_and: [
                    {observed_at: {_lt: $date4}},
                    {observed_at: {_gt: $early}}
                ]},
                {node_id: {_eq: $nodeid4}}
            ]}
        ]
    },
    {
        _not: {
        _and: [
            {status: {_is_null: true}},
            {node_id: {_eq: $nodeid4}}
        ]
        }
    },
    {
        _not: {
        _and: [
            {status: {_eq: "Enter"}},
            {node_id: {_eq: $nodeid4}}
        ]
        }
    }
     {
        _not: {
        _and: [
            {status: {_eq: "Exit"}},
            {node_id: {_eq: $nodeid3}}
        ]
        }
    },
    ]
}) {
    observed_at
    code
    oid
    node_id
    status
    node {
    name
    }
    one_order {
    one_number, 
    one_code
    }
    
}
}
"""


class PieceObservationDelayedPieces:
    def process_late_pieces(hasura):
        res.utils.logger.warning(f"\n\n\n\nBeginning process_late_pieces ...")

        sync_cache = res.connectors.load("redis")["AIRTABLE_CACHE"]["SYNC_DATES"]

        env = os.environ.get("RES_ENV", "development")

        def _make_akey(env, key):
            return f"{env}-{key}"

        key_name = "Delayed_Pieces_Earliest_Sync_Hasura"
        last_sync_date = datetime(2023, 1, 1).astimezone(pytz.utc)

        key = _make_akey(env, key_name)

        earliest_sync_date_from_cache = sync_cache[key]
        if not earliest_sync_date_from_cache:
            earliest_sync_date_from_cache = last_sync_date

        res.utils.logger.warning(
            f"rediskey:{key}  -- earliest_sync_date_from_cache: {earliest_sync_date_from_cache}"
        )

        now = datetime.now(pytz.utc)
        day_ago = (now - timedelta(days=1)).replace(tzinfo=pytz.utc)
        week_ago = (now - timedelta(days=7)).replace(tzinfo=pytz.utc)
        day_ago_str = day_ago.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        earliest_less_1day = earliest_sync_date_from_cache - timedelta(days=1)

        earliest_less_1day = earliest_less_1day.strftime("%Y-%m-%dT%H:%M:%S.%f%z")

        retval2 = hasura.tenacious_execute_with_kwargs(
            GET_DELAYED_PIECES_Q,
            nodeid1="0741bd516d8db6721ffc7f6d7885063a",  # roll packing
            date1=day_ago_str,
            nodeid2="b67d1d6cb3aa303122b000e4916308be",  # printer
            date2=day_ago_str,
            nodeid3="8be3ef2feb9b381ea0cb01fbcc6348f0",  # roll inspection
            date3=day_ago_str,
            nodeid4="3f9f9263-630e-eb35-805f-4ed18f847289",  # cutter
            date4=day_ago_str,
            early=earliest_less_1day,
        )

        res.utils.logger.warning(
            f"delayed pieces from hasura, {earliest_less_1day}  to {day_ago}, returned length: {len(retval2['make_one_pieces'])}"
        )

        piece_observation_logging.statsd_logger_delayed_pieces(
            len(retval2["make_one_pieces"])
        )

        latepiece_response_dicts = {}

        for piece in retval2["make_one_pieces"]:
            #

            piece["one_code"] = piece["one_order"]["one_code"]
            piece["one_number"] = piece["one_order"]["one_number"]
            piece["node"] = piece["node"]["name"]

            piece["delayed_printer_1days"] = []
            piece["delayed_printer_7days"] = []
            piece["delayed_rollpacking_1days"] = []
            piece["delayed_rollpacking_7days"] = []
            piece["delayed_rollinpsection_1days"] = []
            piece["delayed_rollinspection_7days"] = []

            piece["observed_at"] = datetime.strptime(
                piece["observed_at"], "%Y-%m-%dT%H:%M:%S.%f%z"
            ).replace(tzinfo=pytz.utc)

            if piece["node"] == "Make.Print.Printer":
                if piece["observed_at"] < week_ago:
                    piece["delayed_printer_7days"].append(piece["code"])
                elif piece["observed_at"] < day_ago:
                    piece["delayed_printer_1days"].append(piece["code"])

            if piece["node"] == "Make.Print.RollPacking":
                if piece["observed_at"] < week_ago:
                    piece["delayed_rollpacking_7days"].append(piece["code"])
                elif piece["observed_at"] < day_ago:
                    piece["delayed_rollpacking_1days"].append(piece["code"])

            if piece["node"] == "Make.Print.RollInspection":
                if piece["observed_at"] < week_ago:
                    piece["delayed_rollinspection_7days"].append(piece["code"])
                elif piece["observed_at"] < day_ago:
                    piece["delayed_rollinspection_1days"].append(piece["code"])

            piece["observed_at"] = piece["observed_at"].strftime(
                "%Y-%m-%dT%H:%M:%S.%f%z"
            )

            resp = OnePieceLateAtNodeResponse(**(piece))

            latepiece_resp = latepiece_response_dicts.get(resp.one_number)

            if latepiece_resp != None:
                latepiece_resp.delayed_printer_1days.extend(resp.delayed_printer_1days)
                latepiece_resp.delayed_printer_7days.extend(resp.delayed_printer_7days)
                latepiece_resp.delayed_rollpacking_1days.extend(
                    resp.delayed_rollpacking_1days
                )
                latepiece_resp.delayed_rollpacking_7days.extend(
                    resp.delayed_rollpacking_7days
                )
                latepiece_resp.delayed_rollinpsection_1days.extend(
                    resp.delayed_rollinpsection_1days
                )
                latepiece_resp.delayed_rollinspection_7days.extend(
                    resp.delayed_rollinspection_7days
                )
            else:
                latepiece_response_dicts[resp.one_number] = resp

        api = FlowAPI(
            OnePieceLateAtNodeResponse, response_type=OnePieceLateAtNodeResponse
        )

        res.utils.logger.warning(
            f"Compressed to one numbers with delays: {len(latepiece_response_dicts.values())}"
        )

        piece_observation_logging.statsd_logger_delayed_ones(
            len(latepiece_response_dicts.values())
        )

        for resp in latepiece_response_dicts.values():
            api.update_airtable(resp)

            res.utils.logger.warning(
                f"Updated: {resp.one_number}, delayed_printer_1days: {len(resp.delayed_printer_1days)}, delayed_printer_7days: {len(resp.delayed_printer_7days)}, delayed_rollpacking_1days:"
            )
            res.utils.logger.warning(
                f"{len(resp.delayed_rollpacking_1days)}, delayed_rollpacking_7days: {len(resp.delayed_rollpacking_7days)}, delayed_rollinpsection_1days {len(resp.delayed_rollinpsection_1days)}, delayed_rollinspection_7days {len(resp.delayed_rollinspection_7days)}"
            )

        res.utils.logger.warning(f"Logging next sync time: {now}")
        sync_cache[key] = now
