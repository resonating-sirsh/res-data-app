import uuid
import res
from res.flows.make.production.piece_observation_utilities.piece_observation_airtable_to_hasura import (
    PieceObservationAirtableToHasura,
)
from res.flows.make.production.piece_observation_utilities.piece_observation_delayed_pieces import (
    PieceObservationDelayedPieces,
)
from res.flows.make.production.piece_observation_utilities.piece_observation_history_update import (
    PieceObservationHistoryUpdate,
)
from res.flows.make.production.piece_observation_utilities import (
    piece_observation_logging,
)
from schemas.pydantic.make import OnePieceSetUpdateRequest, OnePieceSetUpdateResponse
from res.flows.api import FlowAPI
from gql.transport.exceptions import TransportQueryError
import json
import pandas as pd
import res.flows.make.production
import traceback


from schemas.pydantic.make import *

from datetime import datetime, timedelta, timezone

from warnings import filterwarnings


REQUEST_TOPIC = "res_make.piece_tracking.make_piece_observation_request"
list_missing_orders = []
list_corrupt_piece_codes = []
list_pieces_missing_on_ones = []


def write_missing_orders_to_s3(s3, key):
    now = datetime.now()

    # Format it as a string
    formatted_now = now.strftime("%y-%m-%d-%H-%M-%S")
    missingOrdersPath = (
        "s3://res-data-platform/res-make-missing-orders/missing_ones_v2.csv"
    )
    missingOrdersThisRunPath = f"s3://res-data-platform/res-make-missing-orders/per_job/missing_ones_run_{key}_{formatted_now}_.csv"
    corruptPiecesPath = (
        "s3://res-data-platform/res-make-missing-orders/corrupt_piece_codes.csv"
    )
    missingPiecesPath = (
        "s3://res-data-platform/res-make-missing-orders/missing_pieces_on_one.csv"
    )
    res.utils.logger.warning(f"\nReading {missingOrdersPath} ...")

    existing = s3.read(missingOrdersPath)

    existing_corrupt_pieces = s3.read(corruptPiecesPath)
    existing_missing_pieces = s3.read(missingPiecesPath)

    current = pd.DataFrame(list_missing_orders)
    combined_data = pd.concat([current, existing], ignore_index=True)

    res.utils.logger.warning(
        f"Updating with  {len(list_missing_orders)} missing orders from this run ..."
    )
    combined_data = combined_data.drop_duplicates(subset=["one_number"], keep="last")
    res.utils.logger.warning(
        f"Wrting combined {len(combined_data)} missing orders to {missingOrdersPath} ..."
    )
    res.utils.logger.warning(
        f"Wrting current {len(current)} missing orders to {missingOrdersThisRunPath} ..."
    )
    s3.write(
        missingOrdersPath,
        combined_data,
        index=False,
    )

    s3.write(
        missingOrdersThisRunPath,
        current,
        index=False,
    )

    current_corrupt_pieces = pd.DataFrame(list_corrupt_piece_codes)
    combined_data_corrupt_pieces = pd.concat(
        [current_corrupt_pieces, existing_corrupt_pieces], ignore_index=True
    )
    res.utils.logger.warning(
        f"Updating with  {len(list_corrupt_piece_codes)} corrupt piece codes from this run ..."
    )
    combined_data_corrupt_pieces = combined_data_corrupt_pieces.drop_duplicates()
    res.utils.logger.warning(
        f"Wrting combined {len(combined_data_corrupt_pieces)} missing orders to {corruptPiecesPath}..."
    )
    s3.write(
        corruptPiecesPath,
        combined_data_corrupt_pieces,
        index=False,  # 01483741328
    )

    current_missing_pieces = pd.DataFrame(list_pieces_missing_on_ones)
    combined_current_missing_pieces = pd.concat(
        [current_missing_pieces, existing_missing_pieces], ignore_index=True
    )
    res.utils.logger.warning(
        f"Updating with  {len(current_missing_pieces)} ones that are missing piece codes from this run ..."
    )
    combined_current_missing_pieces = combined_current_missing_pieces.drop_duplicates()
    res.utils.logger.warning(
        f"Wrting combined {len(combined_current_missing_pieces)} missing pieces on Ones to {missingPiecesPath}..."
    )
    s3.write(
        missingPiecesPath,
        combined_current_missing_pieces,
        index=False,
    )


def handle_event(payload: OnePieceSetUpdateRequest, hasura, api, fc, postgres=None):
    try:
        if check_order_exists_in_hasura(payload.one_number, hasura):
            set_of_pieces = set()
            for piece in payload.pieces[:]:
                if len(piece.code) < 5:
                    list_corrupt_piece_codes.append(
                        {
                            "observed_at": piece.observed_at,
                            "one_number": payload.one_number,
                            "code": piece.code,
                            "metadata": json.dumps(payload.metadata),
                            "node": payload.node,
                        }
                    )

                    piece_observation_logging.statsd_logger_invalid_piece_code()
                    payload.pieces.remove(piece)
                    res.utils.logger.warn(
                        f"Removed piece code piece.code: '{piece.code}' from One: {payload.one_number}, observed_at: {payload.observed_at} node: {payload.node} as the piece code appeas invalid"
                    )
                else:
                    piece_uuid = uuid_str_from_dict(piece.dict())
                    if piece_uuid in set_of_pieces:
                        res.utils.logger.warn(
                            f"Removed duplicate from list of pieces in observation piece.code: '{piece.code}' from One: {payload.one_number}, observed_at: {payload.observed_at} node: {payload.node} "
                        )
                        payload.pieces.remove(piece)
                    else:
                        set_of_pieces.add(piece_uuid)

            check_pieceob_is_newer_than_existing(payload, postgres)

            if payload.pieces:  # ensure at least one piece observation left to process
                res.utils.logger.warn(f"Calling api.update on payload: {payload}")
                val = api.update(payload)
                res.utils.logger.warn(
                    f"Successfully updated order with one_number: {payload.one_number}, observed_at: {payload.observed_at}"
                )
                res.utils.logger.warn(
                    f"Updating piece history for: {payload.one_number}"
                )

                PieceObservationHistoryUpdate.onepiece_node_history_update(
                    payload, hasura
                )

                piece_observation_logging.statsd_logger(payload)
            else:
                res.utils.logger.warn(
                    f"zero pieces remaining on message: {payload.one_number}, no updates performed"
                )
        else:
            res.utils.logger.warn(
                f"Order not found in hasura: {payload.one_number}, no updates performed"
            )

            now_utc = datetime.now(timezone.utc)

            list_missing_orders.append(
                {
                    "timestamp": now_utc.strftime("%Y-%m-%d %H:%M:%S"),
                    "one_number": payload.one_number,
                }
            )

            raise Exception(
                f"One missing in hasura, one:{payload.one_number}, observed_at:{payload.observed_at}, node:{payload.node}"
            )

    except Exception as ex:
        codes = [piece.code for piece in payload.pieces]
        codes_csv = ",".join(codes)
        exTrace = traceback.format_exc()
        meta = {
            "one_number": payload.one_number,
            "codes": codes_csv,
            "exception": exTrace,
        }

        if (
            'Foreign key violation. insert or update on table "one_pieces" violates foreign key constraint "one_pieces_one_order_id_fkey"'
            in str(ex)
        ):
            list_pieces_missing_on_ones.append(
                {
                    "one_number": payload.one_number,
                    "codes": codes_csv,
                    "observed_at": payload.observed_at,
                    "node": payload.node,
                }
            )

        fc.queue_exception_metric_inc()
        # Send to dead letter with max of 10 attempt
        fc.write_dead_letter(
            REQUEST_TOPIC,
            payload.dict(),
            exception_trace=meta,
            error_after_counter=10,
        )
        format_message = json.dumps(payload.dict(), indent=4)
        res.utils.logger.warn(
            f"order {payload.one_number} failed to update via flow API. \n\n\n\nKafka Message: {format_message}.  \n\n\n\nException: {ex}"
        )


def handler(event, context={}):
    """
    for now running on a schedule
    """
    try:
        filterwarnings("ignore")
        with res.flows.FlowContext(event, context) as fc:
            res.utils.logger.info(f"event recieved: {event}")
            if len(fc.assets):
                res.utils.logger.info(
                    f"Assets supplied by payload - using instead of kafka"
                )  # nudge nudge
                assets = fc.assets_dataframe
            else:
                usesnowflake = (
                    event.get("args").get("load_from_snowflake") == True
                )  # set this to true if testing locally to read messages from snowflake kafka sink

                s3 = res.connectors.load("s3")

                if usesnowflake:
                    res.utils.logger.info(
                        f"WARNING: loading kaf messages from snowflake.  THis should not be used for production"
                    )
                    snowflake = res.connectors.load("snowflake")
                    if (
                        event.get("args").get("snowflake_query")
                        and len(event.get("args").get("snowflake_query")) > 0
                    ):
                        snowflake_query = event.get("args").get("snowflake_query")
                    else:
                        snowflake_query = """SELECT parse_json(RECORD_METADATA):offset, * FROM "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_MAKE_PIECE_TRACKING_MAKE_PIECE_OBSERVATION_REQUEST"  """
                        if event.get("args").get("start_observation_date") and len(
                            event.get("args").get("start_observation_date")
                        ):
                            start_date = event.get("args").get("start_observation_date")
                            end_date = event.get("args").get("end_observation_date")

                            snowflake_query += f""" where TO_DATE(parse_json(RECORD_CONTENT):observed_at)  >= '{start_date}' and  TO_DATE(parse_json(RECORD_CONTENT):observed_at)  <= '{end_date}'  """
                            filter_ones = event.get("args").get("filter_ones")
                            if filter_ones:
                                snowflake_query += f"AND parse_json(RECORD_CONTENT):one_number IN ({','.join(map(str, filter_ones))})"

                        snowflake_query += (
                            f""" ORDER BY parse_json(RECORD_METADATA):offset """
                        )

                    res.utils.logger.info(f"SNowflake Query: {snowflake_query}")
                    data = snowflake.execute(
                        snowflake_query
                        # """ SELECT top 10 * FROM "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_MAKE_PIECE_TRACKING_MAKE_PIECE_OBSERVATION_REQUEST"  """
                        # """select * from "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_MAKE_PIECE_TRACKING_MAKE_PIECE_OBSERVATION_REQUEST" where parse_json(RECORD_CONTENT):contracts_failed != '[]' """
                        # """select top 500 * from "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_MAKE_PIECE_TRACKING_MAKE_PIECE_OBSERVATION_REQUEST" where parse_json(RECORD_CONTENT):one_number  = 10288332 """
                        # parse_json(RECORD_CONTENT):one_number IN (10295912,10296107,10296244,10296317,10296318,10296319,10296320,10296321,10296322,10296324,10296325,10296338,10296345,10296348,10296351,10296353,10296354,10296355,10296389,10296393,10296398,10296465,10296486,10296487,10296584,10296615,10296617,10296627,10296660,10296708,10296753,10296770,10296805,10296837,10296927,10296946,10296947,10296949,10296954,10297045,10297049,10297129,10297140,10297141)
                        # -- AND
                        #    --LENGTH(pieces.value:code::STRING) < 6"""
                        # LATERAL FLATTEN(input => parse_json(RECORD_CONTENT):pieces) AS pieces
                        # (10298334,10298337,10298347,10298349,10298353,10298355,10298360,10298362,10298363,10298366,10298367,10298370,10298374,10298376,10298378,10298391,10298393,10298395,10298397,10298403,10298404,10298406,10298408,10298410,10298411,10298412,10298413,10298419,10298422,10298425,
                        #    10298426,10298428,10298431,10298434,10298436,10298439,10298441,10298443,10298479,10298481,10298482,10298483,10298484,10298489,10298490,10298500,10298502,10298505,10298506
                        # where parse_json(RECORD_CONTENT):one_number  = 10311307
                        # """SELECT  *
                        #   FROM "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_MAKE_PIECE_TRACKING_MAKE_PIECE_OBSERVATION_REQUEST"   where parse_json(RECORD_CONTENT):one_number  = 10299371
                    )
                    res.utils.logger.info(
                        f"Snowflake load complete, records: {len(data)}"
                    )
                    data["payload"] = data["RECORD_CONTENT"].map(json.loads)
                    assets = pd.DataFrame([d for d in data["payload"]])
                    # assets = assets.sort_values("one_number")

                else:
                    res.utils.logger.info(f"Consuming from topic {REQUEST_TOPIC}")

                    kafka = res.connectors.load("kafka", group_id="res-data")
                    assets = kafka[REQUEST_TOPIC].consume(give_up_after_records=200)

                    res.utils.logger.info(
                        f"Consumed batch size {len(assets)} from topic {REQUEST_TOPIC}"
                    )

                    if not assets.empty:
                        # get now in utc as string
                        now_utc = datetime.now(timezone.utc)
                        # convert to string
                        now_utc_str = now_utc.strftime("%Y%m%d-%H-%M-%S")
                        # generate uuid and convert first 6 chars to string
                        uuid_str = str(uuid.uuid4())[:4]

                        s3.write(
                            f"s3://res-data-platform/res-make-piece-observation/kafka-messages-pulled-by-piece-observation_v2/MAKE-PIECE-OBSERVATION-REQUEST-messages-pulled-{now_utc_str}-{uuid_str}-snowflake-{usesnowflake}.csv",
                            assets,
                            index=False,
                        )

                hasura = res.connectors.load("hasura")

                api = FlowAPI(
                    OnePieceSetUpdateRequest, response_type=OnePieceSetUpdateResponse
                )
                asset_processed_count = 0
                import time

                postgres = event.get("args").get("postgres")
                postgres = postgres or res.connectors.load("postgres")
                # partition will simply group by one pieces with the same status and sets for contracts and defects
                for partition in OnePieceSetUpdateRequest.partition_payloads(
                    assets, re_repartition=False
                ):
                    res.utils.logger.warn(
                        f"Processing One_num:{partition.one_number}, observed_at:{partition.observed_at}, assetNum:{asset_processed_count}"
                    )
                    start = time.time()
                    handle_event(
                        partition,
                        hasura,
                        api,
                        fc,
                        postgres,
                    )
                    end = time.time()
                    res.utils.logger.info(
                        f"time for updates, One: {partition.one_number} observed_at: {partition.observed_at}  pieces: {len(partition.pieces)} ms: {int((end - start) * 1000)} assetnum:{asset_processed_count} of {assets.shape[0]}"
                    )

                    asset_processed_count += 1
                    if asset_processed_count % 1200 == 0:
                        write_missing_orders_to_s3(s3, fc.key)

                postgres.close()
                write_missing_orders_to_s3(s3, fc.key)

        PieceObservationAirtableToHasura.process_user_fields_airtable_to_hasura(hasura)

        PieceObservationDelayedPieces.process_late_pieces(hasura)

        return {}
    except Exception as e:
        # show full stack trace

        res.utils.logger.warn(
            f"\n\n\n\n\n**ERROR**: Top level Exception at the level of the handler: {e}"
        )
        return {}


def check_pieceob_is_newer_than_existing(payload, postgres):
    postgres = postgres or res.connectors.load("postgres")
    query = f"""SELECT DISTINCT
                op.code, op.observed_at
                from make.one_orders oo
                INNER JOIN make.one_pieces op on op.one_order_id = oo.id
                where oo.one_number = {payload.one_number}  
                order by  op.observed_at desc, op.code desc """
    df_one_currentstate = postgres.run_query(query, keep_conn_open=True)
    postgresOnePieces = set(df_one_currentstate.code)

    kafkaOnePieces = set([piece.code for piece in payload.pieces])
    notInPostgres = kafkaOnePieces.difference(postgresOnePieces)
    res.utils.logger.info(
        f"CurrentPiecesForOne: One number: {payload.one_number} currently has the following pieces in postgres ({len(postgresOnePieces)}) : {postgresOnePieces})"
    )
    res.utils.logger.info(
        f"CurrentPiecesForKafMessage: One number: {payload.one_number} currently has the following pieces in kafka: ({len(postgresOnePieces)}) : {kafkaOnePieces})"
    )
    if notInPostgres:
        res.utils.logger.warn(
            f"Extra Pieces found in payload that are not in postgres: {payload.one_number}  codes not In Postgres:{notInPostgres}.  Removing them from observation so the other pieces can update..."
        )

        list_pieces_missing_on_ones.append(
            {
                "one_number": payload.one_number,
                "codes": ",".join(notInPostgres),
                "observed_at": payload.observed_at,
                "node": payload.node,
            }
        )

    for piece in payload.pieces[:]:
        if piece.code in notInPostgres:
            payload.pieces.remove(piece)
            res.utils.logger.warn(
                f"removing {piece.code} from {payload.one_number} kaf message prior to update because this piece did not exist in postgres and would cause update to fail "
            )
        else:
            observed_at_postgres = df_one_currentstate[
                df_one_currentstate["code"] == piece.code
            ]["observed_at"]

            if (
                pd.notna(observed_at_postgres).all()
                and observed_at_postgres.shape[0] > 0
            ):
                observed_at_postgres = safe_convert_timestamp_toutc(
                    observed_at_postgres.iloc[0]
                )
            else:
                res.utils.logger.warn(
                    f"{piece.code} from {payload.one_number} had a NULL timestamp in postgres - assinging 1/1/1900 for comparison purposes "
                )
                observed_at_postgres = safe_convert_timestamp_toutc("01/01/1900")

            if safe_convert_timestamp_toutc(payload.observed_at) < observed_at_postgres:
                res.utils.logger.warn(
                    f"removing {piece.code} from {payload.one_number} kaf message prior to update because observed_at_postgres: {observed_at_postgres} > kaf message observedat: {payload.observed_at} "
                )
                payload.pieces.remove(piece)


def safe_convert_timestamp_toutc(datetimestring):
    timestamp = pd.Timestamp(datetimestring)

    # Check if the timestamp has a timezone
    if timestamp.tz is not None:
        # If it has a timezone, use tz_convert to convert it to the desired timezone
        timestamp = timestamp.tz_convert("UTC")
    else:
        # If it doesn't have a timezone, use tz_localize to set the timezone
        timestamp = timestamp.tz_localize("UTC")
    return timestamp


def check_order_exists_in_hasura(one_number, hasura):
    checkOrderExists = """query get_order_by_one_number($one_number: Int = 10) {
        make_one_pieces(where: {one_order: {one_number: {_eq: $one_number}}, deleted_at: {_is_null: true}}) {
        one_order {
        oid
        one_code
        one_number
        order_number
        sku
        }
        code
        contracts_failed
        defects
        id
        oid
        one_order_id
        node {
        name
        }
    }
        }"""
    data = hasura.execute_with_kwargs(checkOrderExists, one_number=one_number)
    # does an order exist for this one number, AND do all pieces returned have their OID set - oid is used as the constraint for the upsert
    if len(list(data.values())[0]) > 0 and all(
        piece["oid"] is not None for piece in data["make_one_pieces"]
    ):
        return True
    else:
        return False
