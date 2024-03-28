"""Helper ABCs for type checking."""
from __future__ import annotations
from abc import ABC
from collections.abc import Sized
from typing import Any


class Truthy(ABC):
    """ABC for checking Truthiness."""

    @classmethod
    def __instancecheck__(cls, instance: Any) -> bool:
        """If the instance is truthy return True."""
        return bool(instance)


class Falsy(ABC):
    """ABC for checking Falsiness."""

    @classmethod
    def __instancecheck__(cls, instance: Any) -> Any:
        """If the instance is truthy return False."""
        return not bool(instance)


class Empty(ABC):
    """ABC for type-checking if something is empty."""

    @classmethod
    def __instancecheck__(cls, instance: Any) -> bool:
        """Check that instance has length 0 if Sized, else check its dict if present."""
        if isinstance(instance, Sized):
            return len(instance) == 0
        if hasattr(instance, "__dict__"):
            return isinstance(instance.__dict__, cls)
        raise NotImplementedError(f"Not sure how to check if {instance!r} is Empty!")
