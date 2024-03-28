"""Pydantic models for the kafka message dump system."""
from __future__ import annotations
from datetime import datetime
from enum import Enum
import os
from typing import Generic, List, Literal, Optional, Tuple, TypeVar

from uuid import UUID

from pydantic import BaseModel, Extra, Field, PositiveFloat, constr
from schemas.pydantic.common import (
    CommonBaseModel,
    CommonGenericModel,
    CommonConfig,
    MetadataMixin,
    UTCDatetime,
)

Key = TypeVar("Key", None, dict, BaseModel)
Value = TypeVar("Value", None, dict, BaseModel)


def get_tag():
    """Get RES_TAG from the env."""
    return os.getenv("RES_TAG", "latest")


class VerboseKafkaMessage(CommonGenericModel, Generic[Key, Value]):
    """Pydantic model for message metadata."""

    latency: Optional[float]
    len: int
    topic: str
    offset: int = 0
    partition: int = 0
    timestamp: datetime = UTCDatetime
    timestamp_code: Literal[0, 1, 2]
    timestamp_type: str
    headers: List[Tuple[str, str]]
    key: Optional[Key]
    raw_key: Optional[bytes]
    value: Optional[Value]
    raw_value: Optional[bytes]
    exception: Optional[Exception] = Field(None, exclude=True)
    error: Optional[dict]
    error_code: Optional[str]
    error_is_fatal: Optional[bool]
    error_is_retriable: Optional[bool]
    error_name: Optional[str]
    error_str: Optional[str]


class KafkaMessageBase(CommonGenericModel, Generic[Key, Value]):
    """Pydantic base class for kafka message representations with fields o the PK."""

    topic: str
    partition: int = 0
    offset: int = 0
    key: Optional[Key] = None
    value: Optional[Value]

    @property
    def primary_key(self):
        """Return a dict of the PK fields."""
        return self.dict(include={"topic", "partition", "offset"})


class ProcessorSpec(CommonBaseModel):
    """Pydantic model for the processor spec."""

    processor_name: str
    processor_type: str
    git_sha: constr(min_length=6, max_length=40) = Field(  # type: ignore
        default_factory=get_tag
    )

    def __init__(self, processor=None, /, **data):
        """Compute ProcessorSpec for a Processor (usually a TopicHandler)."""
        if processor is not None:
            super().__init__(
                **{
                    "processor_name": repr(processor),
                    "processor_type": type(processor).__qualname__,
                }
            )
        else:
            super().__init__(**data)


class KafkaMessageDumpInput(
    MetadataMixin, KafkaMessageBase[Key, Value], Generic[Key, Value]
):
    """Pydantic model for message dump query input."""

    headers: Optional[List[Tuple[str, str]]]
    length: int = Field(alias="len")
    target_processor: Optional[ProcessorSpec] = None
    timestamp_code: Literal[0, 1, 2]
    timestamp_type: str
    timestamp: datetime = UTCDatetime
    metadata: KafkaMessageDumpInput.Metadata

    class Metadata(MetadataMixin.Metadata):
        """Pydantic model for message metadata."""

        latency: Optional[PositiveFloat]
        length: int = Field(0, alias="len")
        topic: str
        offset: int
        partition: int
        timestamp: datetime = UTCDatetime
        timestamp_code: Literal[0, 1, 2]
        timestamp_type: str
        raw_key: Optional[bytes]
        raw_value: Optional[bytes]
        exception: Optional[Exception] = Field(None, exclude=True)
        error_code: Optional[str]
        error_is_fatal: Optional[bool]
        error_is_retriable: Optional[bool]
        error_name: Optional[str]
        error_str: Optional[str]

        class Config(MetadataMixin.Metadata.Config):
            """Config for KafkaMessage Metadata class."""

            extra: Extra = Extra.allow
            allow_population_by_field_name = True

    class Config(CommonConfig):
        """Config for KafkaMessage Metadata class."""

        allow_population_by_field_name = True


class ProcessingState(Enum):
    """Processing state values for the processing input."""

    # Message was received by the dump table and is awaiting processing
    RECEIVED = "RECEIVED"
    # The message was not able to be successfully passed to the processor.
    FAILED = "FAILED"
    # The message was passed to the event trigger.
    TRIGGERED = "TRIGGERED"
    # The message was accepted by the event trigger and is being processed.
    TRIGGERED_ACCEPTED = "TRIGGERED_ACCEPTED"
    # The message was rejected by the event trigger, but is available to be passed to
    # other event triggers.
    TRIGGERED_REJECTED = "TRIGGERED_REJECTED"
    # The message was processed by the event trigger and successfully ingested.
    # It may be cleared.
    PROCESSED_SUCCEEDED = "PROCESSED_SUCCEEDED"
    # The message was processed by the event trigger, but failed for some reason.
    # See processing_message
    PROCESSED_FAILED_FINAL = "PROCESSED_FAILED_FINAL"


class KafkaMessageProcessingInput(CommonBaseModel):
    """Pydantic model for message proceesing update input."""

    topic: str
    partition: int = 0
    offset: int = 0
    processing_state: ProcessingState = ProcessingState.RECEIVED
    processor: Optional[ProcessorSpec]
    processor_lock: Optional[UUID] = None
    processing_message: Optional[str]
    target_processor: Optional[ProcessorSpec]


class VersionHistoryRecord(KafkaMessageDumpInput[Key, Value], Generic[Key, Value]):
    """Pydantic model for records in version_history."""

    receipt_no: int = 0
    value_changed: bool
    updated_at: datetime = UTCDatetime


class ProcessingHistoryRecord(CommonBaseModel):
    """Pydantic model for records in processing_history."""

    state: ProcessingState
    message: Optional[str]
    processor: Optional[ProcessorSpec]
    processor_lock: Optional[UUID] = None
    target_processor: Optional[ProcessorSpec]
    transitioned_at: datetime = UTCDatetime


class ProcessingUpdate(KafkaMessageProcessingInput):
    """Pydantic model for Processing Update mutation responses."""

    processing_history: List[ProcessingHistoryRecord] = []
    processing_state_last_transitioned_at: datetime = UTCDatetime


class KafkaMessage(KafkaMessageDumpInput[Key, Value], Generic[Key, Value]):
    """Pydantic model for the full Kafka Message table repr."""

    processing_state: Optional[ProcessingState]
    processor: Optional[ProcessorSpec]
    processing_message: Optional[str]
    processor_lock: Optional[UUID] = None
    num_receipts: int = 0
    value_changed: bool = False
    version_history: List[VersionHistoryRecord[Key, Value]] = []
    processing_history: List[ProcessingHistoryRecord] = []
    processing_state_last_transitioned_at: datetime = UTCDatetime
    created_at: datetime = UTCDatetime
    updated_at: datetime = UTCDatetime
