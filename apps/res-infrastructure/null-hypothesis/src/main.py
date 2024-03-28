"""Entry module for the null-hypothesis system."""
from __future__ import annotations
from typing import Any
import os
from pathlib import Path

from fastapi import FastAPI

from res.utils.asyncio import acompose_next, apply_next
from res.utils import logger, secrets_client

from schemas.pydantic.hasura_event_hooks import EventPayload
from schemas.pydantic.kafka_dump_models import KafkaMessage, ProcessingState
from schemas.pydantic.make import (
    PieceSet,
    PrintJob,
    MSJobinfo,
    MaterialSolution,
    NestedOne,
    Printfile,
    MakeProductionRequest,
)

from src.topic_handlers import (
    TopicHandler,
    TopicProcessor,
    ParseIntoHasuraTopicHandler,
    MutationParser,
    get_hasura_client,
)

app = FastAPI()


# stuff to handle print piece responses where we want to use different mutations depending on the
# kafka message
PPR_PIECE_SET_INSERT = MutationParser.ensure_mutation(
    Path(__file__).parent / "queries" / "MakePieceSet.gql"
)
PPR_HEALING_PIECE_INSERT = MutationParser.ensure_mutation(
    Path(__file__).parent / "queries" / "UpsertHealingPiece.gql"
)


async def ppr_handler(message: KafkaMessage):
    value = message.value
    if value is None:
        raise ValueError(f"Got unprocessable message {message!r}")
    logger.info(f"Type of message value ({type(value)!r})")
    parsed = PieceSet(**value)
    if parsed.metadata.rank.lower() != "healing":
        logger.debug(
            f"Inserting piece_set: {parsed.input()}",
        )
        return await get_hasura_client().execute_async(
            PPR_PIECE_SET_INSERT.to_document(), parsed.input()
        )
    else:
        # munge the piece instance to have a lame piece set associated to it so that hasura can figure out the piece set id for us
        graphql_input = {
            "objects": [
                {
                    **piece.input(),
                    "healing": True,
                    "piece_set": {
                        "data": {
                            "one_number": piece.one_number,
                            "key": piece.piece_set_key,
                        },
                        "on_conflict": {
                            "constraint": "piece_sets_key_key",
                            "update_columns": "key",
                        },
                    },
                }
                for piece in parsed.piece_instances
            ]
        }
        logger.debug(
            f"Inserting healing pieces: {graphql_input}",
        )
        return await get_hasura_client().execute_async(
            PPR_HEALING_PIECE_INSERT.to_document(), graphql_input
        )


TopicHandler("res_meta.dxa.prep_pieces_responses", ppr_handler)


ParseIntoHasuraTopicHandler[PrintJob](
    "res_premises.printer_data_collector.print_job_data",
    MSJobinfo,
    Path(__file__).parent / "queries" / "UpsertPrintJob.gql",
)

ParseIntoHasuraTopicHandler[MakeProductionRequest](
    "res_make.make_one_request.create",
    MakeProductionRequest,
    Path(__file__).parent / "queries" / "InsertMakeProductionRequest.gql",
)

ParseIntoHasuraTopicHandler[MaterialSolution](
    "res_make.res_nest.roll_solutions",
    MaterialSolution,
    Path(__file__).parent / "queries" / "UpsertMaterialSolution.gql",
)

ParseIntoHasuraTopicHandler[NestedOne](
    "res_make.res_nest.one_nest",
    NestedOne,
    Path(__file__).parent / "queries" / "UpsertNestedOne.gql",
)

ParseIntoHasuraTopicHandler[NestedOne](
    "res_make.optimus.printfile_pieces",
    Printfile,
    Path(__file__).parent / "queries" / "UpsertPrintfile.gql",
)


@app.on_event("startup")
def initialize_secrets():
    """Initialize hasura secrets from AWS."""
    if os.getenv("RES_ENV") != "local":
        if "HASURA_API_SECRET_KEY" not in os.environ:
            secrets_client.get_secret("HASURA_API_SECRET_KEY")
        if "HASURA_ENDPOINT" not in os.environ:
            secrets_client.get_secret("HASURA_ENDPOINT")


def respond_with_error(
    reason: str, payload: EventPayload, *, send_update=True, **extras
):
    """Log an error and respond with an error return value."""
    reason = f"Unprocessable Event: {reason} ({payload.id}: {payload.event!r})"
    logger.error(reason, **extras)
    if send_update:
        apply_next(
            f"respond_with_error: {reason}",
            TopicProcessor.update,
            payload,
            processing_state=ProcessingState.FAILED,
            processing_message=reason,
        )
    return {"result": "error", "reason": reason, "payload_id": payload.id, **extras}


@app.get("/")
@app.get("/healthcheck")
async def healthcheck():
    return {"status": "ok"}


@app.post("/event-trigger")
async def handle_event(payload: EventPayload[KafkaMessage[None, Any]]):
    """Retrieve handler from TopicProcessor's registry and call it with the payload."""
    if payload.event.data.new is None:
        return respond_with_error(
            "Event's KafkaMessage was deleted or empty.", payload, send_update=False
        )

    topic = payload.event.data.new.topic
    logger.info(f"Received event hook for topic '{topic}'!", payload_id=payload.id)
    if payload.event.data.new.value is None:
        return respond_with_error(
            f"KafkaMessage's value was deleted or empty on {topic}.", payload
        )

    if payload.event.data.new.processing_state in [
        ProcessingState.PROCESSED_SUCCEEDED,
        ProcessingState.PROCESSED_FAILED_FINAL,
    ]:
        processed_at = (
            payload.event.data.new.processing_state_last_transitioned_at.isoformat()
        )
        return {
            "result": "success",
            "note": f"Previously processed at {processed_at}.",
            "payload_id": payload.id,
        }

    handler = TopicProcessor.get_handler(topic)
    if not callable(handler):
        return respond_with_error(
            f"No topic handler registered for '{topic}'.", payload
        )
    acompose_next(handler, TopicProcessor.update)(
        payload, processing_state=ProcessingState.TRIGGERED
    )
    return {"result": "success", "payload_id": payload.id}
