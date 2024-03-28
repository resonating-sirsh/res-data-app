"""Classes & functions for handling partitioning ops on strings."""
from __future__ import annotations
from enum import Enum
from functools import partial, singledispatch
from itertools import chain
from operator import add, lt, itemgetter, methodcaller
from types import DynamicClassAttribute
from typing import Callable, Generic, Literal, Optional, Tuple, TypeVar, Union

from compose import compose

from res.utils.fp import (
    apply,
    constant,
    identity_unpacked,
    if_,
    juxt,
    notinstancesof,
    right_partial,
)

T = TypeVar("T")

SB = TypeVar("SB", bytes, str)
TSB = Tuple[SB, ...]
SB_TSB = Union[SB, TSB]
TSB1 = Tuple[SB]
TSB2 = Tuple[SB, SB]
TSB3 = Tuple[SB, SB, SB]


class Direction(Enum):
    """An enum for tracking left vs. right."""

    LEFT = "left"
    RIGHT = "right"
    is_LEFT: bool
    is_RIGHT: bool

    def __eq__(self, rhs: Union[Direction, str]) -> bool:
        """Make equality check the value."""
        if isinstance(rhs, str):
            return self.value == rhs
        return super().__eq__(rhs)

    def __getattr__(self, name: str) -> bool:
        """Overload instance getattr to allow comparison to other members."""
        # in python >= 3.9 this is str.removeprefix
        if name.startswith("is_"):
            name = name[3:]
        member = self.__class__.__members__.get(name)
        if member is not None:
            return self == member
        if name in self.__class__.__members__.values():
            return self is self.__class__(name)
        raise AttributeError(
            f"'{self.__class__.__name__} object has no attribute '{name}'"
        )


@singledispatch
def make_finder_for(char, direction) -> Callable[[SB], int]:
    """Only str, bytes, and tuple[str|bytes] supported."""
    raise NotImplementedError(f"No finder registered for '{type(char)!r}'")


@make_finder_for.register(str)
@make_finder_for.register(bytes)
def make_finder_for_strs(
    char: SB, direction: Direction = Direction.LEFT
) -> Callable[[SB], int]:
    """Delegate to str.find or str.rfind."""
    return methodcaller("find" if direction.is_LEFT else "rfind", char)


@make_finder_for.register(tuple)
def make_finder_for_tuples(
    char: TSB, direction: Direction = Direction.LEFT
) -> Callable[[SB], int]:
    """Return a finder that finds the left- or rightmost char."""
    if len(char) == 1:
        return make_finder_for_strs(char, direction)
    return compose(
        partial(min if direction.is_LEFT else max, default=-1),
        partial(filter, compose(partial(lt, 0))),
        juxt(
            *map(partial(methodcaller, "find" if direction.is_LEFT else "rfind"), char)
        ),
    )


@singledispatch
def make_partitioner_for(sep, direction) -> Callable[[SB], TSB3]:
    """Only str, bytes, and tuple[str|bytes] supported."""
    raise NotImplementedError(f"No partitioner registered for '{type(sep)!r}'")


@make_partitioner_for.register(str)
@make_partitioner_for.register(bytes)
def make_partitioner_for_strs(
    sep: SB, direction: Direction = Direction.LEFT
) -> Callable[[SB], TSB3]:
    """Delegate to str.partition or str.rpartition."""
    return methodcaller("partition" if direction.is_LEFT else "rpartition", sep)


@make_partitioner_for.register(tuple)
def make_partitioner_for_tuples(
    sep: TSB, direction: Direction = Direction.LEFT
) -> Callable[[SB], TSB3]:
    """Return a partitioner that partitions string at the left- or rightmost sep."""
    if len(sep) == 1:
        return make_partitioner_for_strs(sep, direction)
    return compose(
        partial(apply, apply),  # i.e. str -> (str[:idx], str[idx], str[idx + 1:])
        juxt(
            compose(
                partial(apply, itemgetter),
                partial(apply, if_),
                juxt(
                    partial(lt, 0),  # if idx > 0, then first, else second:
                    juxt(
                        partial(slice),
                        identity_unpacked,
                        compose(right_partial(slice, None), right_partial(add, 1)),
                    ),
                    juxt(
                        *map(
                            compose(constant, slice),
                            (None, 0, 0) if direction.is_LEFT else (0, 0, None),
                        )
                    ),
                ),
                make_finder_for_tuples(sep, direction),
            ),
            identity_unpacked,
        ),
    )


def partition(string: SB, /, sep: Optional[SB_TSB] = None) -> TSB3:
    """Partitition string over sep."""
    return make_partitioner_for(sep, Direction.LEFT)(string)


def rpartition(string: SB, /, sep: Optional[SB_TSB] = None) -> TSB3:
    """Right-partitition string over sep."""
    return make_partitioner_for(sep, Direction.RIGHT)(string)


class PartitionerMeta(type):
    """Metaclass to allow different args for classmethods."""

    def __getattr__(cls, name):
        """Return static partition & rpartition methods."""
        if name == "partition":
            return partition
        if name == "rpartition":
            return rpartition
        raise AttributeError(f"'{cls.__qualname__}' object has no attribute '{name}'")


class Partitioner(Generic[SB], metaclass=PartitionerMeta):
    """Class for handling string partitions."""

    __slots__ = "direction", "sep", "string", "__dict__", "__weakref__"

    sep: TSB
    string: SB
    _original_partitions_: Optional[Partitioner[SB]]

    def __new__(
        cls,
        string: Union[Partitioner[SB], SB],
        sep: Optional[SB_TSB] = None,
        direction: Union[Direction, Literal["left", "right"]] = "left",
        **_,
    ):
        """Instantiate a new Partitioner of a string to iteratively partition it.

        Provides `partition`/`rpartition` that act like
        `str.partition`/`str.rpartition`, but will split at the first/last encountered
        separator character from `sep`.

        If `string` is a Partitioner or PartitionIterator, creates a new instance from
        its internals.
        """
        dict_update = {}
        if isinstance(string, (Partitioner, PartitionIterator)):
            # hold onto the passed Partitioner's `_original_partitions_` or the
            # Partitioner itself
            dict_update["_original_partitions_"] = getattr(
                string, "_original_partitions_", string
            )
            sep = string.sep
            direction = string.direction
            string = string.string

        elif not isinstance(string, (str, bytes)):
            raise TypeError(
                f"Don't know how to partition '{type(string).__qualname__}' object! "
                "(should be 'str' or 'bytes')"
            )

        if sep is not None:
            _sep: TSB = sep if isinstance(sep, tuple) else (sep,)
            bad_type_seps = tuple(notinstancesof(type(string), _sep))
            if any(bad_type_seps):
                raise TypeError(
                    f"Got separator(s) of an incompatible type(s)! {bad_type_seps!r} "
                    f"are not '{type(string)}' objects!"
                )
        else:
            _sep = (b" " if isinstance(string, bytes) else " ",)

        self = super().__new__(cls)
        self.string = string
        self.sep = _sep
        self.direction = Direction(direction)

        if dict_update:
            self.__dict__.update(dict_update)
        return self

    def __init__(self, *_, **__):
        """Make super happy."""
        super().__init__()

    @DynamicClassAttribute
    def partition(self) -> Callable[[], TSB3]:
        """Return a function that does partitioning over self.string."""
        return compose(
            make_partitioner_for(self.sep, Direction.LEFT),
            partial(identity_unpacked, self.string),
        )

    @DynamicClassAttribute
    def rpartition(self) -> Callable[[], TSB3]:
        """Return a function that does rpartitioning over self.string."""
        return compose(
            make_partitioner_for(self.sep, Direction.RIGHT),
            partial(identity_unpacked, self.string),
        )

    def __iter__(self):
        """Return a PartitionIterator for self."""
        return PartitionIterator(self)


class PartitionIterator(Partitioner[SB], Generic[SB]):
    """Iterator class for Partitioner."""

    __slots__ = "left", "center", "right"

    direction: Direction
    left: SB
    center: SB
    right: SB
    string: SB

    def __init__(self, *_, **__):
        """Create a new Iterator over Partitions of a string."""
        super().__init__()
        if self.string is None:
            raise ValueError("Can't iterate over partitions without string!")
        SBcls = type(self.string)
        self.left, self.center, self.right = (
            (SBcls(), SBcls(), self.string)
            if self.direction.is_LEFT
            else (self.string, SBcls(), SBcls())
        )

    def __iter__(self):
        """Return self if iter() called."""
        return self

    def __next__(self) -> TSB2:
        """Return the next partition and the separator, if found."""
        if not self.string:
            raise StopIteration()
        if self.direction.is_LEFT:
            self.left, self.center, self.right = self.partition()
            self.string = self.right
            return self.left, self.center  # type: ignore
        else:
            self.left, self.center, self.right = self.rpartition()
            self.string = self.left
            return self.center, self.right  # type: ignore

    def reset(self) -> Partitioner:
        """Reset the iterator by returning a new Partitions object."""
        return Partitioner(self)


@singledispatch
def capwords(string, _=None):
    """Only str or bytes allowed."""
    raise TypeError("Argument to capwords must be 'str' or 'bytes'")


@capwords.register
def capwords_str(string: str, sep: Union[str, Tuple[str, ...]] = " ") -> str:
    """Capitalize words like string.capwords."""
    return "".join(
        chain.from_iterable(
            (word.capitalize(), sep_char)
            for word, sep_char in Partitioner(string, sep=sep, direction="left")
        )
    )


@capwords.register
def capwords_bytes(string: bytes, sep: Union[bytes, Tuple[bytes, ...]] = b" ") -> bytes:
    """Capitalize words like string.capwords."""
    return b"".join(
        chain.from_iterable(
            (word.capitalize(), sep_char)
            for word, sep_char in Partitioner(string, sep=sep, direction="left")
        )
    )
