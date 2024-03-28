import res
from schemas.pydantic.make import OnePieceSetUpdateRequest
import res.flows.make.production


def statsd_logger(payload: OnePieceSetUpdateRequest):
    try:
        res.utils.logger.metric_node_state_transition_incr(
            payload.node,
            "None",
            payload.status,
            flow="make_piece_observation_request_at_node",
            inc_value=len(payload.pieces),
        )
    except Exception as ex:
        res.utils.logger.warn(
            f"piece_observation statsd metrics attempt to log pieces failed, exception: {ex}"
        )


def statsd_logger_invalid_piece_code():
    try:
        res.utils.logger.metric_node_state_transition_incr(
            "invalid_piece_code_len_less_5",
            "None",
            "None",
            flow="make_piece_observation_request_at_node",
            inc_value=1,
        )
    except Exception as ex:
        res.utils.logger.warn(
            f"piece_observation statsd metrics attempt to log def statsd_logger_invalid_piece_code:, exception: {ex}"
        )


def statsd_logger_delayed_pieces(pieces_count: int):
    try:
        res.utils.logger.metric_node_state_transition_incr(
            "delayed_pieces_returned_from_hasura",
            "None",
            "None",
            flow="make_piece_observation_request_at_node",
            inc_value=pieces_count,
        )
    except Exception as ex:
        res.utils.logger.warn(
            f"piece_observation statsd metrics attempt to log statsd_logger_delayed_pieces, exception: {ex}"
        )


def statsd_logger_airtable_updates_saved_to_hasura(success: bool):
    try:
        # create success of failure string based on value of success
        if success:
            success = "success"
        else:
            success = "failed"

        res.utils.logger.metric_node_state_transition_incr(
            node="One Order Responses",
            asset_key="airtable_updates_saved_to_hasura",
            status=success,
            flow="user_updates_airtable_to_hasura",
            process="piece_observations",
        )
    except Exception as ex:
        res.utils.logger.warn(
            f"piece_observation statsd metrics attempt to log statsd_logger_delayed_pieces, exception: {ex}"
        )


def statsd_logger_delayed_ones(orders_count: int):
    try:
        res.utils.logger.metric_node_state_transition_incr(
            "aggregated_delayed_orders_returned_from_hasura",
            "None",
            "None",
            flow="make_piece_observation_request_at_node",
            inc_value=orders_count,
        )
    except Exception as ex:
        res.utils.logger.warn(
            f"piece_observation statsd metrics attempt to log statsd_logger_delayed_ones, exception: {ex}"
        )


def statsd_logger_airtable_zero_updates_from_user(airtable_name: str):
    try:
        res.utils.logger.metric_node_state_transition_incr(
            node=airtable_name,
            asset_key="read_changes_from_user_airtable",
            status="zero_updates_from_user",
            flow="user_updates_airtable_to_hasura",
            process="piece_observations",
        )
    except Exception as ex:
        res.utils.logger.warn(
            f"piece_observation statsd metrics attempt to log read_changes_from_user_airtable, exception: {ex}"
        )


def statsd_logger_airtable_updates_from_user(airtable_name: str, counter: int):
    try:
        res.utils.logger.metric_node_state_transition_incr(
            node=airtable_name,
            asset_key="read_changes_from_user_airtable",
            status="updates_from_user",
            flow="user_updates_airtable_to_hasura",
            process="piece_observations",
            inc_value=counter,
        )
    except Exception as ex:
        res.utils.logger.warn(
            f"piece_observation statsd metrics attempt to log read_changes_from_user_airtable, exception: {ex}"
        )
