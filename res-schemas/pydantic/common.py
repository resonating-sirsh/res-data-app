"""Common classes for pydantic models."""
from __future__ import annotations
from base64 import b64encode
from copy import deepcopy
from datetime import datetime
from enum import Enum
from functools import lru_cache, partial
from itertools import chain, starmap
import json
from operator import attrgetter, eq
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TYPE_CHECKING,
    TypeVar,
    Union,
    get_origin,
    overload,
)
from uuid import UUID

from typing_extensions import Annotated, Concatenate, ParamSpec

from compose import compose
import orjson
from pydantic import (
    BaseConfig,
    BaseModel,
    Extra,
    ExtraError,
    Field,
    ValidationError,
    root_validator,
)
from pydantic.class_validators import make_generic_validator
from pydantic.error_wrappers import ErrorWrapper
from pydantic.fields import ModelField
from pydantic.generics import GenericModel
from pydantic.types import _registered

from res.utils.fp import (
    constant,
    filter_empty,
    get_mapper_for,
    identity_unpacked,
    instancesof,
    juxt,
    map_by,
    right_partial,
    safe_attrgetter,
)
from res.utils.members import public_vars
from res.utils.logging import logger
from res.utils.type_checking import Empty, Falsy
from stringcase import titlecase

import hashlib


def uuid_str_from_dict(d):
    """
    generate a uuid string from a seed that is a sorted dict
    """
    d = json.dumps(d, sort_keys=True).encode("utf-8")
    m = hashlib.md5()
    m.update(d)
    return str(UUID(m.hexdigest()))


def id_mapper_factory(fields, alias={}):
    """
    prepares a dictionary from fields. For example kwargs could be a dict of type
    fields is a sub set of fields in the type that are used to generate a hash of a dictionary of this key:values

    if alias passed in we generate the dict from the alias.
    for example, suppose we use the user email as part of the key
    on our table user might be called "owner" and have a value of type email
    on the users table the key is generated from email:value
    therefore we generate an id saying we expect a key to be generated from email:[owners_email]

    example:
     fields = ['owner'], alias = {'owner' : 'email'}

    all of this in service of having business keys that will always map to ids without round trips to the database
    we test all the pydantic types and then use them ubiquitously
    """

    def f(**kwargs):
        d = {alias.get(k, k): v for k, v in kwargs.items() if k in fields}
        return uuid_str_from_dict(d)

    return f


StringOrBytes = Union[bytes, str]

# Some geoms - is there another way??
GPoint = List[Union[int, float]]
GLine = List[GPoint]
GMultiLine = List[GLine]
GMultiPoint = List[GPoint]


UTCDatetime = Field(default_factory=datetime.utcnow)

UTCDatetimeString = Field(
    default_factory=datetime.utcnow().replace(tzinfo=None).isoformat
)


P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")

H = TypeVar("H", bound="HasuraModel")
SKIP_PROPAGATION = object()


def orjson_dumps(val, *, default):
    """Dump the val to a string using orjson, and decode the resulting bytes."""
    return orjson.dumps(val, default=default).decode()  # pylint: disable=no-member


class UpdateError(ValidationError):
    """Exception class for extra.forbid errors in update method."""

    def __str__(self) -> str:
        """Ensure the string representation says update error."""
        return super().__str__().replace("validation error", "update error")


if TYPE_CHECKING:
    # SerializesToJson[list[str]] will be recognized by type checkers as list[str]
    SerializesToJson = Annotated[T, ...]

else:

    class SerializesToJson:
        def __class_getitem__(cls, inner_type: Type[Any]):
            if inner_type is Any:
                # allow SerializesToJson[Any] to replecate plain SerializesToJson
                return cls
            return _registered(
                type(
                    "SerializesToJsonValue",
                    (cls,),
                    {"inner_type": inner_type},
                )
            )

        @classmethod
        def __get_validators__(cls):
            yield cls.validate

        @classmethod
        def validate(cls, value):
            """Return the value."""
            return value


LazyTypeResolver = Callable[[], Type]
TypeSerializerKey = Union[Type, LazyTypeResolver]


class SerializableMixin:
    """BaseModel subclass that serializes timestamps in dict() calls."""

    # pylint: disable=unnecessary-lambda
    _field_serializers_: ClassVar[Dict[str, Callable[[Any], Any]]] = {}
    _type_serializers_: ClassVar[Dict[TypeSerializerKey, Callable[..., Any]]] = {
        bytes: lambda v: str(b64encode(v), encoding="utf-8", errors="replace"),
        set: lambda v: list(v),
        tuple: lambda v: list(v),
        datetime: lambda v: v.isoformat(),
        UUID: lambda v: str(v),
        Enum: lambda v: v.value,
        SerializesToJson: partial(orjson_dumps, default=json.dumps),
    }
    _default_serializer_: Optional[Callable[[Any], Any]] = None

    @classmethod
    @lru_cache()
    def resolve_type_serializer(
        cls,
        typ: TypeSerializerKey,
        ser: Callable[..., Any],
    ) -> Tuple[type, Callable[..., Any]]:
        if not callable(typ):
            raise TypeError(
                f"Keys of type_serializers dicts must be callable! Got: {typ}"
            )
        if isinstance(typ, type):
            return typ, ser
        return typ(), ser

    def __init_subclass__(cls, **kwargs) -> None:
        """Intercept the dict method to add DatetimeSerializing logic."""
        super().__init_subclass__(**kwargs)
        if not (issubclass(cls, BaseModel) and issubclass(cls, SearchMROMixin)):
            raise TypeError(
                f"{SerializableMixin.__name__} is only usable with pydantic.BaseModel"
                f"-derived classes with the SearchMROMixin! ({cls!r})"
            )

    @classmethod
    def __resolve_serializers__(cls):
        """Resolve all serializers up the mro."""
        assert issubclass(cls, SearchMROMixin)

        def _inner(name):
            return chain.from_iterable(
                reversed(
                    tuple(
                        filter_empty(
                            map(
                                dict.items,
                                chain.from_iterable(
                                    cls.__search_mro__(
                                        f"Config.{name}_serializers",
                                        f"_{name}_serializers_",
                                        default={},
                                    )
                                ),
                            ),
                            include_empty=False,
                        )
                    )
                )
            )

        return (
            dict(_inner("field")),
            dict(starmap(cls.resolve_type_serializer, _inner("type"))),
            filter(
                callable,
                cls.__search_mro__(
                    "_default_serializer_", "Config.default_serializer", default=None
                ),
            ),
        )

    @classmethod
    def _get_value(cls, val, to_dict: bool, *args, **kwargs) -> Any:
        if isinstance(val, SerializableMixin):
            if to_dict:
                v_dict = val.serializable_dict(*args, **kwargs)
                if "__root__" in v_dict:
                    return v_dict["__root__"]
                return v_dict
        return super(cls)._get_value(val, *args, to_dict=to_dict, **kwargs)

    def serializable_iter(
        self,
        *,
        by_alias=False,
        exclude_none=False,
        exclude_unset=False,
        exclude_defaults=False,
        **kw,
    ):
        """Get an iter over serializable keys and values."""
        assert isinstance(self, BaseModel)
        serializers = type(self).__resolve_serializers__()

        for field_key, val in self._iter(
            by_alias=by_alias,
            to_dict=False,
            exclude_none=exclude_none,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            **kw,
        ):
            if field_key not in self.__fields_set__ and exclude_none:
                continue
            field = self.__fields__.get(field_key)
            serialized = (
                val.serializable_dict()
                if isinstance(val, SerializableMixin)
                else self._serialize_value_(val, field, *serializers)
            )
            if serialized is None and exclude_none:
                continue
            if field and exclude_defaults:
                if serialized == field.default:
                    continue
            dict_key = field.alias if field and by_alias else field_key
            yield dict_key, serialized

    def serializable_dict(self, **kw):
        """Get dict from super and serialize datetime fields."""
        assert isinstance(self, BaseModel)
        return dict(self.serializable_iter(**kw))

    def _get_serializer_(
        self,
        value: Any,
        field: Optional[ModelField],
        field_serializers: dict,
        type_serializers: dict,
        default_serializers: filter,
    ) -> Optional[Callable]:
        assert isinstance(self, BaseModel)
        serializer = None
        if field:
            # try field specific serializers
            try:
                serializer = next(
                    filter(
                        callable,
                        (
                            field.field_info.extra.get("serializer"),
                            field_serializers.get(field.name),
                            field_serializers.get(field.alias),
                        ),
                    )
                )
                logger.debug(
                    f"Using field serializer for {value!r}",
                    field=field,
                    serializer=serializer,
                    cls=self.__class__,
                )
            except StopIteration:
                pass
        # try serializers who match something on the MRO
        if not serializer:
            try:
                serializer = next(
                    filter(
                        callable,
                        starmap(
                            lambda typ, ser: ser if isinstance(value, typ) else None,
                            type_serializers.items(),
                        ),
                    )
                )
                logger.debug(
                    f"Using type serializer for {value!r}",
                    type=type(value).__name__,
                    serializer=serializer,
                    cls=self.__class__,
                )
            except StopIteration:
                pass
        # try the default serializer
        if not serializer:
            try:
                serializer = next(default_serializers)
                logger.debug(
                    f"Using default serializer for {value!r}",
                    serializer=serializer,
                    cls=self.__class__,
                )
            except StopIteration:
                pass
        return serializer

    def _serialize_value_(
        self, value: Any, field: Optional[ModelField], *serializers
    ) -> Any:
        assert isinstance(self, BaseModel)
        ser = self._get_serializer_(value, field, *serializers)
        if ser is not None:
            return make_generic_validator(ser)(
                type(self), value, self, field, self.Config  # type: ignore
            )
        return value


class FieldsFromMixin:
    """Mixin that adds the __fields_from__ method."""

    @overload
    @classmethod
    def __fields_from__(
        cls, field_names: Optional[Union[Empty, Falsy]]
    ) -> Generator[ModelField, None, None]:
        """Return an empty generator if  args are passed."""

    @overload
    @classmethod
    def __fields_from__(
        cls, field_names: Iterable[str]
    ) -> Generator[ModelField, None, None]:
        """Resolve the fields listed in `field_names`."""

    @overload
    @classmethod
    def __fields_from__(
        cls,
        field_names: Optional[Iterable[str]],
        func: Callable[Concatenate[ModelField, P], R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Generator[R, None, None]:
        """Map the fields listed in `field_names` (or all fields) through `func`."""

    @overload
    @classmethod
    def __fields_from__(
        cls,
        field_names: Optional[Iterable[str]],
        *attrs: str,
        include_empty: bool = False,
        default: Any = None,
    ) -> Generator[Any, None, None]:
        """Get `attrs` from the fields listed in `field_names` (or all fields)."""

    @classmethod
    def __fields_from__(cls, field_names, *args, include_empty=False, **kwargs):
        """Resolve all fields in field_names."""
        assert issubclass(cls, BaseModel)
        if not field_names:
            if not args:
                return
            yield from filter_empty(
                map_by(cls.__fields__.values(), *args, **kwargs), include_empty
            )
        else:
            for name in field_names:
                if name in cls.__fields__:
                    result = get_mapper_for(*args, **kwargs)(cls.__fields__[name])
                    if result or include_empty:
                        yield result
                    continue
                yield from filter_empty(
                    map_by(
                        filter(
                            compose(
                                partial(eq, name), right_partial(getattr, "alias", None)
                            ),
                            cls.__fields__.values(),
                        ),
                        *args,
                        **kwargs,
                    ),
                    include_empty,
                )

    @classmethod
    def values_from(cls, data: Dict[str, Any], *args, field_names=None, **kwargs):
        """Yield fields from the passed data."""
        assert issubclass(cls, BaseModel)
        if field_names and field_names is not None:
            fields_iter = cls.__fields_from__(field_names, *args, **kwargs)
        elif args:
            fields_iter = filter(
                get_mapper_for(*args, **kwargs), cls.__fields__.values()
            )
        else:
            fields_iter = cls.__fields__.values()

        for field in fields_iter:
            if field.name in data:
                yield field.name, data[field.name]
            elif field.alias in data:
                yield field.alias, data[field.alias]

    @classmethod
    def required_values_from(cls, data: Dict[str, Any]):
        """Yield required fields from the passed data."""
        assert issubclass(cls, BaseModel)
        return cls.values_from(data, attrgetter("required"), field_names=None)


class SearchMROMixin:
    """Mixin that adds the __search_mro__ method."""

    @classmethod
    def __search_mro__(cls, *args, include_empty=False, **kwargs):
        """Resolve all fields in field_names."""
        return filter_empty(map_by(cls.__mro__, *args, **kwargs), include_empty)


class UpdatableMixin:
    """Mixin to add the update method."""

    def update(
        self,
        values: Dict[str, Any],
        *,
        extra: Optional[Extra] = None,
        overwrite: bool = True,
    ):
        """Update the record with the values in values, respecting extra."""
        assert isinstance(self, BaseModel)
        if extra is None:
            extra = self.Config.extra
        update_keys = values.keys()
        if update_keys > self.__fields__.keys():
            if extra == Extra.forbid:
                raise UpdateError(
                    [
                        ErrorWrapper(ExtraError(), loc=f)
                        for f in sorted(update_keys - self.__fields__.keys())
                    ],
                    self.__class__,
                )
            if extra == Extra.ignore:
                update_keys &= self.__fields__.keys()

        kv_sources = (self._iter(), ((k, values.get(k)) for k in update_keys))

        values = dict(
            chain.from_iterable(kv_sources if overwrite else reversed(kv_sources))
        )
        fields_set = self.__fields_set__ | values.keys()
        object.__setattr__(self, "__dict__", values)
        object.__setattr__(self, "__fields_set__", fields_set)
        return self


class CommonConfig(BaseConfig, SearchMROMixin):
    """Common settings to extend in Models Config classes."""

    arbitrary_types_allowed = True
    allow_population_by_field_name: bool = True
    extra = Extra.ignore
    allow_mutation = False
    json_dumps: Callable[..., str] = orjson_dumps
    json_loads: Callable[..., str] = orjson.loads  # pylint: disable=no-member


class CommonBaseModel(
    BaseModel,
    SerializableMixin,
    FieldsFromMixin,
    UpdatableMixin,
    SearchMROMixin,
    **public_vars(CommonConfig),
):
    """Common base for our models."""


class CommonGenericModel(
    GenericModel,
    SerializableMixin,
    UpdatableMixin,
    FieldsFromMixin,
    SearchMROMixin,
    **public_vars(CommonConfig),
):
    """Common base for our generic models."""


class MetadataMixin:
    """Mixin class for adding metadata."""

    class Metadata(CommonBaseModel):
        """Placeholder metadata class."""

        class Config(CommonConfig):
            """Config for Metadata Class."""

            extra: Extra = Extra.allow

    @classmethod
    def __excluded_from_metadata_field_names__(cls) -> Set[str]:
        """Resolve all field names that should be excluded from metadata up the mro."""
        assert issubclass(cls, CommonBaseModel) or issubclass(cls, CommonGenericModel)
        return set(
            instancesof(
                str,
                chain.from_iterable(
                    chain.from_iterable(
                        cls.__search_mro__(
                            "Config.excluded_from_metadata_field_names",
                            "_excluded_from_metadata_field_names_",
                            default=set(),
                        )
                    )
                ),
            )
        )

    @classmethod
    def __excluded_from_metadata_fields__(
        cls, *arg, **kw
    ) -> Generator[ModelField, None, None]:
        """Resolve all excluded_from_metadata fields up the mro."""
        assert issubclass(cls, CommonBaseModel) or issubclass(cls, CommonGenericModel)
        return cls.__fields_from__(
            cls.__excluded_from_metadata_field_names__(), *arg, **kw
        )

    def __init__(
        self,
        *,
        metadata: Optional[Union[MetadataMixin.Metadata, Dict[str, Any]]] = None,
        **data,
    ) -> None:
        """Ensure the metadata is pre-poulated on the values (nothing else works).."""
        assert isinstance(self, CommonBaseModel) or isinstance(self, CommonGenericModel)
        field = self.__fields__.get("metadata")
        if field is None:
            raise TypeError(f"{self.__class__!r} has no `metadata` field!")

        super().__init__(
            **{
                **data,
                "metadata": self.populate_metadata(metadata, values=data, field=field),
            }
        )

    def populate_metadata(
        self,
        metadata: Optional[Union[MetadataMixin.Metadata, Dict[str, Any]]] = None,
        *,
        values,
        field: ModelField,
    ) -> MetadataMixin.Metadata:
        """Copy the extra fields into the metadata dict."""
        assert isinstance(self, CommonBaseModel) or isinstance(self, CommonGenericModel)
        populatable = deepcopy(values)
        excluded_fields = set(
            self.__class__.__fields_from__(
                set(field.field_info.extra.get("skip_population", set()))
            )
        ) | set(self.__class__.__excluded_from_metadata_fields__())
        for excluded_field in {field} | excluded_fields:
            populatable.pop(excluded_field.name, None)
            populatable.pop(excluded_field.alias, None)
        if metadata is None:
            return self.__class__.Metadata(**populatable)
        if isinstance(metadata, dict):
            metadata = self.__class__.Metadata(**metadata)
        metadata = metadata.update(populatable, overwrite=False)
        return metadata

    def refresh_metadata(self):
        """Refresh the instance's metadata, e.g. after an update."""
        assert isinstance(self, CommonBaseModel) and "metadata" in self.__fields_set__
        metadata: MetadataMixin.Metadata = self.metadata.copy(deep=True)  # type: ignore
        refreshed = self.populate_metadata(
            metadata,
            values=self.dict(),
            field=self.__fields__["metadata"],
        )
        self.update({"metadata": refreshed})
        return self


class HasuraModel(MetadataMixin, CommonBaseModel):
    """Base model that provides input-sanitized dicts."""

    created_at: Optional[datetime] = UTCDatetime
    updated_at: Optional[datetime] = UTCDatetime

    class Config(CommonConfig):
        """Common config for Hasura Models."""

        allow_population_by_field_name: bool = False
        autogen_field_names: Set[str] = {"created_at", "updated_at"}
        excluded_if_unset_field_names: Set[str] = set()
        extra = Extra.ignore
        excluded_from_metadata_field_names: Set[str] = {
            "id",
            "created_at",
            "metadata",
            "updated_at",
        }
        non_propagated_field_names: Set[str] = {"id", "metadata"}

    @classmethod
    def __autogen_field_names__(cls) -> Set[str]:
        """Resolve all autogenerated field names up the mro."""
        return set(
            instancesof(
                str,
                chain.from_iterable(
                    chain.from_iterable(
                        cls.__search_mro__(
                            "Config.autogen_field_names",
                            "_autogen_field_names_",
                            default=set(),
                        )
                    )
                ),
            )
        )

    @classmethod
    def __excluded_if_unset_field_names__(cls) -> Set[str]:
        """Resolve all excluded if unset field names up the mro."""
        return set(
            instancesof(
                str,
                chain.from_iterable(
                    chain.from_iterable(
                        cls.__search_mro__(
                            "Config.excluded_if_unset_field_names",
                            "_excluded_if_unset_field_names_",
                            default=set(),
                        )
                    )
                ),
            )
        )

    @classmethod
    def __non_propagated_field_names__(cls) -> Set[str]:
        """Resolve all non-propagated field names up the mro."""
        return set(
            instancesof(
                str,
                chain.from_iterable(
                    chain.from_iterable(
                        cls.__search_mro__(
                            "Config.non_propagated_field_names",
                            "_non_propagated_field_names_",
                            default=set(),
                        )
                    )
                ),
            )
        )

    @classmethod
    def __autogen_fields__(cls, *arg, **kw):
        """Resolve all autogenerated fields up the mro."""
        return cls.__fields_from__(cls.__autogen_field_names__(), *arg, **kw)

    @classmethod
    def __excluded_from_metadata_fields__(cls) -> Generator[ModelField, None, None]:
        """Resolve all excluded_from_metadata fields, plus any linked fields."""
        yield from super().__excluded_from_metadata_fields__()
        yield from cls.linked_fields()

    @classmethod
    def __excluded_if_unset_fields__(cls, *arg, **kw):
        """Resolve all excluded if unset fields up the mro."""
        return cls.__fields_from__(cls.__excluded_if_unset_field_names__(), *arg, **kw)

    @classmethod
    def __non_propagated_fields__(cls, *arg, **kw):
        """Resolve all non-propagated fields fields up the mro."""
        return cls.__fields_from__(cls.__non_propagated_field_names__(), *arg, **kw)

    @classmethod
    def linked_fields(cls) -> Iterator[ModelField]:
        """Yield fields representing hasura relationships."""
        return filter(
            compose(
                all,
                juxt(
                    right_partial(isinstance, type),
                    right_partial(issubclass, HasuraModel),
                ),
                safe_attrgetter("type_"),
            ),
            cls.__fields__.values(),
        )

    def input(self):
        """Return a dict of self without autogen'ed fields."""
        exclude = self.__class__.__autogen_field_names__() | (
            self.__class__.__excluded_if_unset_field_names__() - self.__fields_set__
        )
        values = self.serializable_dict(
            exclude=exclude,
            by_alias=False,
        )
        for field in self.__class__.linked_fields():
            linked_type: Type[HasuraModel] = field.type_
            value = getattr(self, field.name)
            logger.info(
                f"Getting input form of linked {field.outer_type_} value "
                f"{type(self).__name__}.{field.name}.",
                value=value,
                linked_type=linked_type,
            )
            relationship_rhs = field.field_info.extra.get("relationship_rhs", None)
            if relationship_rhs is not None:
                if not isinstance(value, list):
                    raise ValueError(
                        "Trying to specify a many to many relationship without a list"
                    )
                rhs_type = getattr(
                    field.type_.__fields__.get(relationship_rhs), "type_", None
                )
                if not issubclass(rhs_type, HasuraModel):
                    raise TypeError(
                        f"{relationship_rhs} must be HasuraModel, got {rhs_type}"
                    )
                values[field.name] = {
                    "data": [
                        {
                            relationship_rhs: {
                                "data": getattr(item, relationship_rhs).input()
                            }
                        }
                        for item in value
                    ]
                }
            elif isinstance(value, list):
                values[field.name] = [item.input() for item in value]
            else:
                values[field.name] = value.input()
        return values

    @classmethod
    def prep_for_propagation(cls, field: ModelField, values: Dict[str, Any]):
        """Deepcopy values and remove any fields that should not propagate to links."""
        propagatable = deepcopy(values)
        for from_, to_, default in field.field_info.extra.get("propagate_rename", []):
            value_from = propagatable.pop(from_, default)
            if value_from is not SKIP_PROPAGATION:
                propagatable[to_] = value_from
        for excluded_field in set(
            chain(
                cls.__non_propagated_fields__(),
                cls.__fields_from__(
                    field.field_info.extra.get("skip_propagating", set())
                ),
                {field},
            )
        ):
            propagatable.pop(excluded_field.name, None)
            propagatable.pop(excluded_field.alias, None)
        return propagatable

    # @root_validator(pre=True, allow_reuse=True, skip_on_failure=False)
    @classmethod
    def linked_records_with_propagted_data(
        cls, values
    ) -> Generator[Tuple[str, Union[Dict[str, Any], List[Dict[str, Any]]]], None, None]:
        """Copy the values into the linked HasuraModel records."""
        # wrapping w/ tuple here forces the `values.pop` side effects to occur first.
        if not cls.linked_fields():
            return values
        for field, field_maybe_values in tuple(
            map(
                compose(
                    tuple,
                    juxt(
                        identity_unpacked,
                        compose(
                            tuple,
                            chain.from_iterable,
                            compose(
                                juxt(
                                    compose(
                                        partial(map, right_partial(values.pop, None)),
                                        safe_attrgetter("alias", "name"),
                                    ),
                                    juxt(
                                        compose(
                                            get_origin, safe_attrgetter("outer_type_")
                                        ),
                                        constant(dict),
                                    ),
                                )
                            ),
                        ),
                    ),
                ),
                cls.linked_fields(),
            )
        ):
            logger.warning(
                f"Maybe values for field {field.name}",
                values=field_maybe_values,
            )
            value = next(filter(None, field_maybe_values))
            if isinstance(value, type):
                value = value()
            linked_type = field.type_
            origin = get_origin(field.outer_type_)
            key = field.alias
            if origin is list and value and not isinstance(value, list):
                value = [value]
            if isinstance(value, list):
                logger.warning(f"Propagating to linked list at {key}", value=value)
                yield key, [
                    linked_type.handle_propagation(
                        item[0], cls.prep_for_propagation(field, values), key
                    )
                    if isinstance(item, list)
                    else linked_type.handle_propagation(
                        item, cls.prep_for_propagation(field, values), key
                    )
                    for item in value
                ]
            else:
                yield key, linked_type.handle_propagation(
                    value, cls.prep_for_propagation(field, values), key
                )

    @classmethod
    def handle_propagation(
        cls, value: Union[HasuraModel, dict], propagatable: Dict[str, Any], key: str
    ):
        """Coerce propagation from a linked hasura model."""
        try:
            return dict(
                cls.values_from(propagatable),
                **(value.dict() if isinstance(value, HasuraModel) else value),
            )
        except TypeError as err:
            logger.warning(f"Got friggin list error at {key}!", err=err, value=value)
            raise

    def __init__(self, **data) -> None:
        propagated_linked_records = {}
        for key, value in self.__class__.linked_records_with_propagted_data(data):
            logger.info(
                f"Saw propagated records for {key} with type {type(value)}",
                size=len(value),
            )
            propagated_linked_records[key] = value
        super().__init__(**{**data, **propagated_linked_records})


class FlowStatus(Enum):
    HOLDING = "Holding"
    QUEUED = "Queued"
    ACTIVE = "Active"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    DONE = "Done"


class FlowApiModel(CommonBaseModel):
    """Base model that provides input-sanitized dicts."""

    created_at: Optional[str] = UTCDatetimeString
    updated_at: Optional[str] = UTCDatetimeString

    # ordinarily we control the ids unless they are uuids
    id: Optional[str]

    class Config(CommonConfig):
        """Common config for Hasura Models."""

        # keep in mind when using config like this we can also add attributes on the properties themselves - pros and cons

        allow_population_by_field_name: bool = False
        autogen_field_names: Set[str] = {"created_at", "updated_at"}
        airtable_attachment_fields: Set[str] = {}
        overide_airtable_table_name = ""
        # for the airtable schema we may want extra control over how types are mapped. we prefer to lean on python types and a handful of conventions though
        airtable_custom_types = {}
        # if you want to rename the fields that are sent to the database you can do this
        # #- some use cases are mapping from FIELD to FIELD_ID as in models you want a lookup but in db you want an FK
        db_rename = {}

        # the system id will actually be aliased to id everywhere but the users id will be kept in metadata or something like that
        # we may pop this into metadata later - note that if it is specified as a key then maybe we can take its value??

    @property
    def key_fields(cls):
        return [
            name
            for name, meta in cls.__fields__.items()
            if meta.field_info.extra.get("key_field")
        ]

    @property
    def airtable_primary_key_field(cls):
        # TODO: there may be some other convention or config to name the primary key
        return titlecase(cls.primary_key_field)

    @property
    def airtable_table_name(cls):
        """
        this like other airtable things could be overrifden by config
        """
        table_name = str(type(cls).__name__)
        return (
            f"{titlecase(table_name)}s"
            if table_name.lower()[-1] != "s"
            else f"{titlecase(table_name)}"
        )

    @property
    def airtable_attachment_fields(cls):
        """ """
        if hasattr(cls.Config, "airtable_attachment_fields"):
            return cls.Config.airtable_attachment_fields

    @property
    def primary_key_field(cls):
        """
        A primary key is a business key that is unique and useful
        in some cases we dont have one so we just use the unique id that is generated
        So for example, if displaying in views like airtable, it may be that the user
        gets a single row Name they care about or they need a combination of columns to determine the match
        """
        chk = [
            name
            for name, meta in cls.__fields__.items()
            if meta.field_info.extra.get("primary_key")
        ]
        return chk[0] if len(chk) else "id"

    @property
    def primary_key(cls):
        return getattr(cls, cls.primary_key_field)

    def airtable_dict(cls, contract_variables=None):
        """
        We can pass contract variables as a first class citizen of about airtable and the flow api
        experimenting with different ways to do this
        depends on possible synced tables and different versions of names so its a little messy
        """

        def coerce_complex_type(v):
            if isinstance(v, dict):
                return str(v)
            return v

        d = cls.dict()
        d = {k: coerce_complex_type(v) for k, v in d.items()}

        """
        CONTRACT VARIABLE SPECIAL CASE MAPPING (TBD)
        """
        if contract_variables is not None:
            field_properties = cls.schema()["properties"]
            # if we have contract variables there is a common synced table we can load records from
            # this might be easier to manage, trying it out
            for k, v in field_properties.items():
                # a more explicit opt in than the name will be better + need to test against existing id mapping schemes
                if v.get("airtable_fk_table") == "Contract Variables":
                    contracts = d[k]
                    # print("mapping contract variables")
                    if isinstance(contracts, list):
                        # cv could support lists which would be more efficient for lookup too
                        contracts = [
                            contract_variables.try_get(item) for item in contracts
                        ]
                        d[k] = [d.record_id for d in contracts if d is not None]

        return d

    def db_dict(cls, rename=None):
        """
        the db dict is a dictionary with possible id and field name mapping

        rename logic could be in the config or on the type
        """

        rename = (
            rename or cls.Config.db_rename if hasattr(cls.Config, "db_rename") else {}
        )

        d = cls.dict()
        field_properties = cls.schema()["properties"]
        # not sure how we transfer properties to aliases TODO think about this
        for _, v in field_properties.items():
            if v.get("alias"):
                field_properties[v["alias"]] = dict(v)
        # im not sure if the schema casing is always f(Title) ->string case

        def m(k, d):
            """
            given a property, and using the entire context of the object, apply the function provide
            """
            prop = field_properties.get(k, {})

            f = prop.get("id_mapper")
            # if we have a function use it to generate an id if we can from the props in the type
            if f:
                return f(**d)
            # or just return the value
            return d[k]

        # we do any renaming and dropping. by default we write all props unless its turned off
        d = {
            rename.get(k, k): m(k, d)
            for k, _ in d.items()
            if field_properties.get(k, {"db_write": True}).get("db_write", True)
        }

        return d

    @root_validator
    def _id(cls, values):
        """
        The id for res flow api is generated as a hash of the key map.
        we use ID_FIELD as _id so that the users id can always be aliased to something else
        """

        ID_FIELD = "id"
        key_fields = [
            name
            for name, meta in cls.__fields__.items()
            if meta.field_info.extra.get("key_field")
        ]
        key_field_map = {k: values.get(k) for k in key_fields}
        # print(key_field_map, type(cls), "in root validator")
        # id_value = values.get(ID_FIELD)
        # we could default it but then we dont know if the intent is to pass the id from payload
        # so instead we specify intent by adding one or more key fields that re used to generate the uuid instead of the one passed in

        if len(key_fields):
            values[ID_FIELD] = uuid_str_from_dict(key_field_map)
        return values

    @classmethod
    def airtable_schema(
        cls, custom_name_mapping={"Created At": "datetime", "Updated At": "datetime"}
    ):
        """
        a mapping from pydantic type information to airtable
        see https://airtable.com/api/enterprise#fieldTypes for field types
        """

        def get_airtable_field_type(ftype, field_spec={}):
            """
            an object or a type can be passed
            """

            d = {}

            airtable_attachment_fields = (
                cls.Config.airtable_attachment_fields
                if hasattr(cls.Config, "airtable_attachment_fields")
                else []
            )
            if field_spec["title"] in airtable_attachment_fields:
                ftype = "attachment"

            custom_types = (
                cls.Config.airtable_custom_types
                if hasattr(cls.Config, "airtable_custom_types")
                else {}
            )
            # any field can be mapped to a custom type
            if field_spec["title"] in custom_types:
                ftype = cls.Config.airtable_custom_types.get(field_spec["title"])

            if field_spec:
                if field_spec.get("airtable_fk_table"):
                    ftype = "record_links"

            if ftype == "email":
                d["type"] = "email"

            elif ftype == "singleCollaborator":
                d["type"] = "singleCollaborator"
            elif ftype == "url":
                d["type"] = "url"
            elif ftype == "attachment":
                d["type"] = "multipleAttachments"
            elif ftype in ["list", "array"]:
                d["type"] = "multipleSelects"
                d["options"] = {"choices": [{"name": "*"}]}
            elif ftype == "record_links":
                d["type"] = "multipleRecordLinks"
                d["options"] = {
                    "linkedTableId": field_spec.get("airtable_fk_table")
                    # view if for record selection is a nice feature
                }
            elif ftype == "integer":
                d["type"] = "number"
                d["options"] = {"precision": 0}
            elif ftype == "float":
                d["type"] = "number"
                d["options"] = {"precision": 2}
            elif ftype == "number":
                d["type"] = "number"
                d["options"] = {"precision": 2}
            elif ftype == "boolean":
                d["type"] = "checkbox"
                d["options"] = {"icon": "check", "color": "greenBright"}

            elif ftype == "datetime":
                d["type"] = "dateTime"
                d["options"] = {
                    "dateFormat": {"name": "iso"},
                    "timeFormat": {"name": "24hour"},
                    "timeZone": "utc",
                }
            elif ftype == "string":
                d["type"] = "singleLineText"
            elif ftype == "dict":
                d["type"] = "singleLineText"
            # complex type coerced
            elif ftype == "object":
                d["type"] = "singleLineText"
            else:
                raise Exception(
                    f"Could not handle the case for type {ftype} with spec {field_spec}"
                )

            return d

        s = cls.schema()
        fields = s["properties"]

        primary_key = [s["title"] for s in fields.values() if s.get("primary_key")]
        if primary_key:
            primary_key = primary_key[0]
        # override with config which is better for base classes especially
        if hasattr(cls.Config, "airtable_primary_key"):
            primary_key = cls.Config.airtable_primary_key

        if primary_key is None:
            raise Exception("The airtable schema must have a primary key field")

        table_name = s["title"]
        # TODO: actually there is a redundancy - this logic exists in the class property also
        table_name = (
            f"{titlecase(table_name)}s"
            if table_name.lower()[-1] != "s"
            else f"{titlecase(table_name)}"
        )
        table_name = getattr(cls.Config, "overridden_airtable_tablename", table_name)

        customs = lambda k: custom_name_mapping.get(k)
        field_spec = {v["title"]: v for k, v in fields.items()}
        # NOTE: we are temp specifying a multi select from an array map if we dont have a type
        # actually this is for handling Unions, Enums etc which need to be tested more
        fields = {
            v["title"]: customs(v["title"]) or v.get("type", "array")
            for k, v in fields.items()
        }

        return {
            "table_name": table_name,
            "key_field": primary_key,
            "_fields_input": fields,
            "fields": {
                k: get_airtable_field_type(v, field_spec[k]) for k, v in fields.items()
            },
        }

    def get_airtable_update_options(cls, **kwargs):
        """
        Should just by convention generate the fields that we send to airtable
        this is a temporary hook as there will be better places to put this

        - we need to know all the types that might not be obvious
        - we need to resolve attachment fields
        - we need to choose names for tables and fields

        """
        d = cls.dict()
        d = {titlecase(k): v for k, v in d.items()}
        # at least remove date times and dicts - we can much smarter about this
        d = {
            k: v if not isinstance(v, datetime) else v.isoformat() for k, v in d.items()
        }
        d = {k: v if not isinstance(v, dict) else str(v) for k, v in d.items()}

        # these are the defaults and they go with how we default schema mapping too. spend some time on these
        return {
            "table_name": cls.airtable_table_name,
            "record": d,
            "key_field": titlecase(cls.primary_key_field),
            "attachment_fields": cls.Config.airtable_attachment_fields,
        }

        return d


class FlowApiQueueStatus(CommonBaseModel):
    """
    In interface on all queues for status items

    """

    id: str
    node: str
    status: str
    contracts_failed: Optional[List[str]]
    trace_log: Optional[str]


class FlowApiAgentMemoryModel(CommonBaseModel):
    """
    An interfaces for pieces of data that we want to add into the agents memory banks
    The idea is to describe how we want to ingest data using emerging patterns for ingestion
    - Statistical data should be written to tables (selectively send fields)
    - KEY value data should be written to Key-value stores (selectively send fields)
    - Long text should be sent into vector stores
    """

    class Config(CommonConfig):
        """Common config for Hasura Models."""

        entity_name = None
        namespace = None

    def columnar_dict(cls):
        """
        restricts to columns that are sensible for tabular search
        """
        s = cls.schema()
        fields = s["properties"]

        fields_we_want = [
            k for k, v in fields.items() if not v.get("is_large_text", False)
        ] + ["id"]
        return {k: v for k, v in cls.dict().items() if k in fields_we_want}

    def attribute_dict(cls):
        """
        restricts to columns that are sensible for key value lookup
        """
        s = cls.schema()
        fields = s["properties"]

        fields_we_want = [
            k
            for k, v in fields.items()
            if not v.get("is_large_text", False)
            and not v.get("is_columnar_only", False)
        ] + ["id"]
        return {k: v for k, v in cls.dict().items() if k in fields_we_want}

    def large_text_dict(cls):
        """
        restricts to columns that are sensible for key value lookup
        """
        s = cls.schema()
        fields = s["properties"]

        fields_we_want = [
            k
            for k, v in fields.items()
            if not v.get("is_large_text", True) or v["type"] == "string"
        ] + ["id"]
        return {k: v for k, v in cls.dict().items() if k in fields_we_want}
