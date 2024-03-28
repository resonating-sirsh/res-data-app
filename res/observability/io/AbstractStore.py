from res.observability.entity import AbstractEntity
from res.observability.dataops import (
    describe_function,
    FunctionDescription,
    FunctionFactory,
)
from res.utils import logger


class AbstractStore:
    def __init__(
        cls, entity: AbstractEntity, alias: str = None, description: str = None
    ):
        # TODO: bit weird - want to figure out of we want to use instances or not
        cls._entity = entity
        cls._alias = alias
        cls._entity_name = cls._entity.entity_name
        cls._entity_namespace = cls._entity.namespace
        cls._key_field = cls._entity.key_field
        cls._fields = cls._entity.fields
        cls._about_entity = cls._entity.about_text
        ###############
        cls.description = description
        cls._summary = ""

    def update_index(cls):
        from res.observability.io.index import update_function_index

        logger.debug(f"adding indexing for new table")
        return update_function_index(cls, summary=cls.description)

    def set_summary(cls, summary):
        cls._summary = summary

    @property
    def name(cls):
        return cls._alias or cls._entity_name

    @property
    def entity_namespace(cls):
        return cls._entity_namespace

    @property
    def entity_name(cls):
        return cls._entity_name

    @property
    def entity(cls):
        return cls._entity

    def load(cls, limit: int = None):
        """
        abstract for now
        """
        return None

    def get_data_model(cls, infer=False):
        """
        by default we use the entity used to create the store but we can infer from stored data too
        """
        return cls.entity

    def as_function_description(cls, context=None, name=None, weight=0):
        """
        Pass in the name and context
        """
        context = (
            context
            or f"Provides context about the type [{cls._entity_name}] - ask your full question as per the function signature. {cls.description}"
        )

        return describe_function(
            cls.run_search,
            alias=name or f"run_search_for_{cls._entity_name}",
            augment_description=context,
            weight=weight,
            # this is a bit lame but doing it for now. we need a proper factory mechanism
            factory=FunctionFactory(
                # for abstract stores this is the name but not in general
                name="run_search",
                weight=weight,
                partial_args={
                    "store_type": str(cls.__class__),
                    "store_name": f"{cls._entity_namespace}.{cls._entity_name}",
                    # on vector stores we need to know the embedding for now - if the store we loaded with meta data e.g. from type resolution we would not
                    "embedding": getattr(cls, "_embedding_provider", None),
                },
            ),
        )

    def as_agent(cls):
        """
        convenience retrieve agent with self as function description to test stores
        """
        from res.learn.agents import InterpreterAgent
        from functools import partial

        return InterpreterAgent(
            initial_functions=[cls.as_function_description()],
            allow_function_search=False,
        )
