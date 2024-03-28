"""Pydantic models for the event hooks system."""
from __future__ import annotations
from typing import Dict, Generic, Literal, TypeVar, Optional, Union
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, Extra, Field, Json
from schemas.pydantic.common import CommonBaseModel, CommonGenericModel, UTCDatetime

T = TypeVar("T", bound=BaseModel)


class Trigger(CommonBaseModel, extra=Extra.allow):
    """The trigger information about the Hasura event."""

    name: str


class Table(CommonBaseModel, extra=Extra.allow):
    """The table information on the Hasura event."""

    schema_name: str = Field(alias="schema")
    name: str


class TraceContext(CommonBaseModel, extra=Extra.allow):
    """The table information on the Hasura event."""

    trace_id: str
    span_id: str


class DeliveryInfo(CommonBaseModel, extra=Extra.allow):
    """The table information on the Hasura event."""

    max_retries: int
    current_retry: int


class EventData(CommonGenericModel, Generic[T]):
    """The event.data field of a Hasura event."""

    old: Optional[T] = None
    new: Optional[T] = None


class Event(CommonGenericModel, Generic[T]):
    """The event field of a Hasura event."""

    op: Literal["INSERT", "UPDATE", "DELETE", "MANUAL"]
    data: EventData[T]
    session_variables: Optional[Dict[str, Union[str, Json]]] = None
    trace_context: Optional[TraceContext] = None


class EventPayload(CommonGenericModel, Generic[T]):
    """The Hasura event trigger payload."""

    id: UUID
    created_at: datetime = UTCDatetime
    event: Event[T]
    delivery_info: Optional[DeliveryInfo]
    trigger: Optional[Trigger]
    table: Optional[Table]
