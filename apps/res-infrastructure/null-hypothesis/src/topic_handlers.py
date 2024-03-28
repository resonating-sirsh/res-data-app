"""Topic handlers module for null-hypothesis.

This is what drives the system.
"""
from __future__ import annotations
from abc import ABCMeta
from asyncio import Task
from collections import abc
from functools import lru_cache, partial, update_wrapper
from itertools import repeat
from operator import attrgetter, is_not
import os
from pathlib import Path
from types import DynamicClassAttribute
from typing import Any, Callable, Dict, Generic, Optional, Tuple, Type, TypeVar, Union
from traceback import format_exception, format_tb

from typing_extensions import Self

from compose import compose
from graphql import (
    DocumentNode,
    FieldNode,
    OperationDefinitionNode,
    OperationType,
    Source,
    TokenKind,
)
from graphql.language.parser import Parser
from res.connectors.graphql.hasura import Client as HasuraClient
from res.utils.asyncio import apply_next, acompose_next
from res.utils.fp import apply, notinstancesof, right_partial, safe_attrgetter
from res.utils.logging import logger
from res.utils.members import SetAndForget
from res.utils.string_partitions import capwords

from schemas.pydantic.hasura_event_hooks import EventPayload
from schemas.pydantic.kafka_dump_models import (
    KafkaMessage,
    KafkaMessageProcessingInput,
    ProcessingState,
    ProcessorSpec,
)
from schemas.pydantic.common import HasuraModel

T = TypeVar("T")
H = TypeVar("H", bound=HasuraModel)


@lru_cache()
def init_hasura_client(*args, **kwargs):
    return HasuraClient(*args, **kwargs)


def get_hasura_client(api_url=None, api_key=None):
    hasura_params = dict(
        api_url=api_url or os.getenv("HASURA_ENDPOINT"),
        api_key=api_key or os.getenv("HASURA_API_SECRET_KEY"),
    )
    logger.info("Initializing HasuraClient")
    return init_hasura_client(**hasura_params)


class MutationDocumentNode(DocumentNode):
    """Class for ensuring mutation GQL docs are actually mutations."""

    definitions: Tuple[OperationDefinitionNode, ...]

    def __init__(self, **kwargs: Any) -> None:
        """Validate the mutation document after creation."""
        super().__init__(**kwargs)
        self.validate()
        self._response_keys: Tuple[str] = tuple(
            selection.name.value
            for definition in self.definitions
            for selection in definition.selection_set.selections
            if isinstance(selection, FieldNode)
        )
        self._mutation_names: Tuple[str] = tuple(
            map(
                compose(
                    next, partial(filter, None), reversed, tuple, partial(map, apply)
                ),
                repeat(("mutation_{}".format, safe_attrgetter("name.value"))),
                enumerate(self.definitions),
            )
        )

    @property
    def response_keys(self) -> Tuple[str]:
        """Return the computed _response_key tuple."""
        return self._response_keys

    @property
    def mutation_names(self) -> Tuple[str]:
        """Return a tuple of the computed name or placeholder for each mutation node."""
        return self._mutation_names

    @classmethod
    def from_document(cls, document: DocumentNode):
        """Convert a document to a MutationDocument."""
        return cls(**{key: getattr(document, key) for key in document.keys})

    def to_document(self) -> DocumentNode:
        """Return the base DocumentNode."""
        return DocumentNode(**{key: getattr(self, key) for key in self.keys})

    def validate(self):
        """Validate the mutation."""
        if not self.definitions:
            raise ValueError("Mutation doc cannot be empty!")
        if any(notinstancesof(OperationDefinitionNode, self.definitions)):
            raise TypeError("Expected mutation doc to have operations, not fragments!")

        if any(
            filter(
                compose(
                    right_partial(is_not, OperationType.MUTATION),
                    attrgetter("operation"),
                ),
                self.definitions,
            )
        ):
            non_mutations = tuple(
                map(
                    lambda operation: f"'{operation.value}'",
                    filter(
                        right_partial(is_not, OperationType.MUTATION),
                        map(attrgetter("operation"), self.definitions),
                    ),
                )
            )
            raise TypeError(
                "Expected mutation doc's operations to be mutations, not "
                f"{' or '.join(non_mutations)}!"
            )


class MutationParser(Parser):
    """Parser subclass to return MutationDocuments."""

    def parse_mutation(self) -> MutationDocumentNode:
        """Parse the document into a MutationDocument and validate it."""
        start = self._lexer.token
        return MutationDocumentNode(
            definitions=self.many(TokenKind.SOF, self.parse_definition, TokenKind.EOF),
            loc=self.loc(start),
        )

    @classmethod
    def parse(
        cls,
        source: Source,
        no_location: bool = False,
    ):
        """Parse the source into a mutation document."""
        return cls(
            source,
            no_location=no_location,
        ).parse_mutation()

    @classmethod
    def from_source(cls, str_or_path: Union[str, Path], **kwargs):
        """Parse the mutation from a string or path."""
        if isinstance(str_or_path, (Path, str)) and Path(str_or_path).exists():
            source = Source(
                body=Path(str_or_path).read_text("utf-8"), name=str(str_or_path)
            )
        elif isinstance(str_or_path, str):
            source = Source(str_or_path, "GraphQL Request")
        elif isinstance(str_or_path, Path):
            raise ValueError(
                f"Recived Path for mutation '{str_or_path!r}', but path does not exist!"
            )
        else:
            raise TypeError(
                "Expected string or Path-like, but got "
                f"'{str_or_path.__class__.__qualname__}' object instead!"
            )
        return cls.parse(source, **kwargs)

    @classmethod
    def ensure_mutation(
        cls, mutation: Union[str, Path, DocumentNode]
    ) -> MutationDocumentNode:
        """Check if the mutation is as expected and return or raise."""
        if isinstance(mutation, DocumentNode):
            return MutationDocumentNode.from_document(mutation)
        return cls.from_source(mutation)


PROCESSOR_RESPONSE = MutationParser.ensure_mutation(
    Path(__file__).parent / "queries" / "ProcessorResponse.gql"
)

_PROCESSOR_REGISTRY_ = {}


def update_with_error(
    reason: str,
    payload: EventPayload,
    processor: TopicProcessor,
    state: ProcessingState,
    **extras,
):
    """Update the message with and log an error."""
    logger.error(reason, **extras)
    return apply_next(
        f"{state.name.lower()}_with_error: {reason}",
        processor.__class__.update,
        payload,
        processing_state=state,
        processor=processor,
        processing_message=reason,
    )


class TopicProcessor(ABCMeta):
    """Metaclass that owns the topic handler registry."""

    def __getattr__(cls, key: str):
        """If the key asked for is _registry_ provide it."""
        if key == "_registry_":
            return _PROCESSOR_REGISTRY_
        getattr(TopicProcessor, key)

    def __new__(
        cls: type[Self],
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> Self:
        """Create the new class."""

        def _registry_(self, *_):
            raise TypeError(
                f"'{self!r}' object cannot access '_registry_'! Use "
                f"'{self.__class__.__name__}._registry_' or 'TopicProcessor._registry_'"
                "instead."
            )

        handler_descriptor = namespace.pop("handler", None)
        if not isinstance(handler_descriptor, (property, abc.Callable)):
            raise TypeError(
                "TopicProcessor types must provide a 'handler' descriptor or method! "
                f"Got '{handler_descriptor}' ({cls!r})"
            )
        if callable(handler_descriptor):
            handler_descriptor = SetAndForget(handler_descriptor)
        elif not isinstance(handler_descriptor, SetAndForget):
            handler_descriptor = SetAndForget(
                handler_descriptor.fget,
                handler_descriptor.fset,
                handler_descriptor.fdel,
                handler_descriptor.__doc__,
            )

        def _validator(*_, **__) -> Tuple[bool, str]:
            """Validate the payload and return reason if rejected."""
            return True, ""

        validator = namespace.pop("validator", _validator)
        if not callable(validator):
            raise TypeError(
                f"TopicProcessor class '{name}' has member 'validator' that is not "
                f"callable. It should be a method. Got '{validator}' ({cls!r})"
            )

        if "__call__" in namespace:
            logger.warning(
                f"TopicProcessor class '{name}' provided a '__call__' method that will "
                "be ignored. Perhaps you meant to define a 'handler' method "
                "instead?"
            )

        async def call(self, payload: Union[Callable, EventPayload[KafkaMessage]]):
            """Call the handler or raise.

            Ensure captured exceptions are logged.
            """
            if callable(payload):
                if handler_descriptor.settable(self):
                    self.handler = payload
                    return self
                raise TypeError(
                    "Handler already set, expected 'KafkaMessage' not "
                    f"'{type(payload)!r}'! (Got: '{payload!r}')"
                )
            if not isinstance(payload, EventPayload):
                reason = (
                    f"Expected EventPayload[KafkaMessage], not '{type(payload)!r}'! "
                    f"(Got: '{payload!r}')"
                )
                return update_with_error(reason, payload, self, ProcessingState.FAILED)
            if not isinstance(payload.event.data.new, KafkaMessage):
                reason = (
                    "Expected message of type KafkaMessage, not "
                    f"'{type(payload.event.data.new)!r}'! "
                    f"(Got: '{payload.event.data.new!r}')"
                )
                return update_with_error(reason, payload, self, ProcessingState.FAILED)
            if not callable(self.handler):
                reason = (
                    "Handler invalid or not set! expected callable method, not"
                    f"'{type(self.handler)!r}'! (Got: '{self.handler!r}')"
                )
                return update_with_error(
                    reason, payload, self, ProcessingState.TRIGGERED_REJECTED
                )

            accepted, reason = self.validator(payload)
            if not accepted:
                reason = f"Rejected Event: {reason} ({payload.id}: {payload.event!r})"
                return update_with_error(
                    reason, payload, self, ProcessingState.TRIGGERED_REJECTED
                )

            return acompose_next(
                self.handler,
                safe_attrgetter("event.data.new"),
                TopicProcessor.update,
            )(
                payload,
                processing_state=ProcessingState.TRIGGERED_ACCEPTED,
                processor=self,
                processing_message=reason,
            ).add_done_callback(
                partial(self.callback, payload)
            )

        def callback(self, payload: EventPayload, task: Task):
            """Update the message with success or failure."""
            try:
                result = task.result()
            except Exception as exc:  # pylint: disable=broad-except
                reason = (
                    f"Failed Event: Error handling message for topic '{self.topic}'"
                    f" ({format_exception(exc)})"
                )
                return update_with_error(
                    reason,
                    payload,
                    self,
                    ProcessingState.PROCESSED_FAILED_FINAL,
                    err_type=repr(type(exc)),
                    err=repr(exc),
                    trace=format_tb(exc.__traceback__),
                )
            logger.debug(
                f"Got result of handling message for topic '{self.topic}'",
                processor_qualname=repr(self),
                message=payload,
                result=result,
            )
            return apply_next(
                f"update_with_success: {self.topic} => ({payload.id})",
                cls.update,
                payload,
                processing_state=ProcessingState.PROCESSED_SUCCEEDED,
                processor=self,
                processing_message=str(result)[:100],
            )

        namespace = {
            **namespace,
            "handler": handler_descriptor,
            "callback": callback,
            "validator": validator,
            "_registry_": DynamicClassAttribute(_registry_, _registry_, _registry_),
            "__call__": call,
        }

        return super().__new__(cls, name, bases, namespace, **kwargs)

    @staticmethod
    def get_handler(topic: str):
        """Get a handler from the registry."""
        return _PROCESSOR_REGISTRY_.get(topic)

    @classmethod
    async def update(
        cls,
        payload: EventPayload[KafkaMessage],
        *,
        processing_message: Optional[str] = None,
        processing_state: ProcessingState = ProcessingState.TRIGGERED,
        processor: Optional[TopicProcessor] = None,
        target_processor: Optional[Union[ProcessorSpec, TopicProcessor]] = None,
    ):
        """Update the message with the processing response."""
        message = payload.event.data.new
        if message is None:
            logger.warn(
                f"Skipping update for event with empty message! {payload.id}",
                processing_message=processing_message,
                processing_state=processing_state,
                processor=processor,
                target_processor=target_processor,
            )
            return
        processor_ = (
            ProcessorSpec(
                processor_name=cls.__qualname__,
                processor_type=TopicProcessor.__qualname__,
            )
            if processor is None
            else ProcessorSpec(processor)
        )
        if isinstance(target_processor, TopicProcessor):
            target_processor = ProcessorSpec(target_processor)
        logger.info(
            f"Updating processing state for message ({message.primary_key})",
            **message.primary_key,
            processing_message=processing_message,
            processing_state=processing_state,
            processor=processor_,
            processor_lock=payload.id,
            target_processor=target_processor,
        )
        processing_input = KafkaMessageProcessingInput(
            topic=message.topic,
            offset=message.offset,
            partition=message.partition,
            processing_message=processing_message,
            processing_state=processing_state,
            processor=processor_,
            processor_lock=payload.id,
            target_processor=target_processor,
        )
        result = await get_hasura_client().execute_async(
            PROCESSOR_RESPONSE.to_document(),
            processing_input.serializable_dict(exclude_unset=True, exclude_none=True),
        )
        processing_update = result[PROCESSOR_RESPONSE.response_keys[0]]
        logger.info(
            f"Got response to processing update for message ({message.primary_key})",
            **processing_update,
        )
        return payload


class TopicHandler(metaclass=TopicProcessor):
    """Topic handler function or pydandic class wrapper."""

    __slots__ = "topic", "func", "__dict__", "__weakref__"
    topic: str
    func: Callable[[KafkaMessage], Any]

    def topic_qualname(self):
        """Return the qualified name of the topic."""
        return (
            capwords(self.topic.replace("-", "_"), sep=(".", "_"))
            .replace("_", "")
            .replace(".", "_")
        )

    def __repr__(self):
        """Name the handler for its ProcessorSpec."""
        return f"(K:{self.topic_qualname()})=>{self.handler_qualname()}"

    def handler_qualname(self):
        """Name the handler for its ProcessorSpec."""
        return f"({self.func.__qualname__})"

    def __init__(self, topic, func: Optional[Callable[[KafkaMessage], Any]] = None):
        """Set the topic and func on self."""
        self.topic = topic
        self.__class__._registry_[topic] = self  # type: ignore
        if callable(func):
            self.handler = func

    @SetAndForget
    def handler(self) -> Optional[Callable[[KafkaMessage], Any]]:
        """Return self.func, if set."""
        return getattr(self, "func", None)

    @handler.setter
    def handler(self, func: Callable[[KafkaMessage], Any]) -> None:
        """Set self.func if unset."""
        if not callable(func):
            raise TypeError(
                f"Don't know what to do with non-callable handler '{func!r}'"
            )
        self.func = func
        update_wrapper(self, func)


class ParseIntoHasuraTopicHandler(TopicHandler, Generic[H]):
    """Default TopicHandler implementation that takes a parsing model and query."""

    __slots__ = "model", "mutation"
    model: Type[H]
    mutation: MutationDocumentNode

    def mutations_qualname(self):
        """Return the names of the mutations."""
        # pylint: disable
        return "|".join(map("{{λ:{}}}".format, self.mutation.mutation_names))

    def handler_qualname(self):
        """Name the handler for its ProcessorSpec."""
        return f"({self.model.__qualname__})=>({self.mutations_qualname()})"

    def __init__(
        self, topic: str, model: Type[H], mutation: Union[DocumentNode, Path, str]
    ):
        """Store the passed model and resolve the graphql query."""
        super().__init__(topic)
        self.model = model
        self.mutation = MutationParser.ensure_mutation(mutation)

    # pylint: disable=method-hidden
    def func(self, parsed: H) -> Dict[str, Any]:
        """Return the input form for `parsed`."""
        return parsed.input()

    async def _handler(
        self, message: Union[Callable[[KafkaMessage], Any], KafkaMessage]
    ):
        """Parse the KafkaMessage's `value` with `model` as and input to `mutation`."""
        value = message.value
        if value is None:
            raise ValueError(f"Got unprocessable message {message!r}")
        logger.info(f"Type of message value ({type(value)!r})")
        parsed = self.model(**value)
        input_ = self.func(parsed)  # type: ignore
        logger.debug(f"Calling mutation: {self.mutations_qualname}", input=input_)
        return await get_hasura_client().execute_async(
            self.mutation.to_document(), input_
        )

    @SetAndForget
    def handler(self) -> Optional[Callable[[KafkaMessage], Any]]:
        """Return the async _handler method."""
        return self._handler

    @handler.setter
    def handler(self, func: Callable[[H], Any]) -> None:
        """Set self.func if unset."""
        if not callable(func):
            raise TypeError(
                f"Don't know what to do with non-callable handler '{func!r}'"
            )
        self.func = func  # type: ignore
        update_wrapper(self._handler, func)
